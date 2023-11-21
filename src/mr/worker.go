package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := FetchTask{}
		var task FetchTaskReply
		//fmt.Println("into")
		failCount := 0
		ok := call("Coordinator.FetchTask", &args, &task)
		if !ok {
			failCount++
			if failCount > 2 {
				return
			}
		}
		failCount = 0
		if ok {
			switch task.TaskType {
			case InitialType:
				time.Sleep(3 * time.Second)
				continue
			case MapTaskType:
				intermediateFileInfo, err := doMapTask(mapf, task)
				if err != nil {
					continue
				}
				reportMapTaskDone(task, intermediateFileInfo)
			case ReduceTaskType:
				//time.Sleep(3 * time.Second)
				if err := doReduceTask(reducef, task); err != nil {
					continue
				}
				reportReduceTaskDone(task)
			default:
				log.Fatal("unknown task type")
			}
		}
	}
}

func reportReduceTaskDone(task FetchTaskReply) {
	args := ReportReduceTaskDoneArgs{
		ReduceTaskID: task.TaskID,
	}
	reply := ReportReduceTaskDoneReply{}
	ok := call("Coordinator.ReportReduceTaskDone", &args, &reply)
	if !ok {
		log.Fatalf("cannot call Coordinator.FetchTask")
	}

}

func doMapTask(mapf func(string, string) []KeyValue, task FetchTaskReply) ([]IntermediateFileInfo, error) {
	intermediate := []KeyValue{}
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("cannot close %v", task.FileName)
		return nil, err
	}

	kva := mapf(task.FileName, string(content))
	intermediate = append(intermediate, kva...)
	return storeMapIntermediate(intermediate, task.TaskID, task.NReduce)
}

func reportMapTaskDone(task FetchTaskReply, intermediateInfo []IntermediateFileInfo) {
	args := ReportMapTaskDoneArgs{
		MapTaskID:            task.TaskID,
		IntermediateFileInfo: intermediateInfo,
		FileName:             task.FileName,
	}
	reply := ReportMapTaskDoneReply{}
	ok := call("Coordinator.ReportMapTaskDone", &args, &reply)
	if !ok {
		log.Fatalf("cannot call Coordinator.FetchTask")
	}
	return
}

func storeMapIntermediate(intermediate []KeyValue, mapTaskID, nReduce int) ([]IntermediateFileInfo, error) {
	// store intermediate for each reduce task
	divideKeyValue := make([][]KeyValue, nReduce)
	for _, value := range intermediate {
		var iHash = ihash(value.Key) % nReduce
		divideKeyValue[iHash] = append(divideKeyValue[iHash], value)
	}

	intermediateFileInfo := make([]IntermediateFileInfo, nReduce)
	for reduceID, intermediateValue := range divideKeyValue {
		fileName := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceID)
		file, err := os.CreateTemp("./", fileName+"-temp")
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
			return nil, err
		}

		enc := json.NewEncoder(file)
		for _, v := range intermediateValue {
			if err := enc.Encode(&v); err != nil {
				log.Fatalf("cannot encode %v", v)
				return nil, err
			}
		}

		if err := os.Rename(file.Name(), "./"+fileName); err != nil {
			log.Fatalf("cannot rename %v", fileName)
			return nil, err
		}

		if err := file.Close(); err != nil {
			log.Fatalf("cannot close %v", fileName)
			return nil, err
		}

		intermediateFileInfo[reduceID] = IntermediateFileInfo{
			ReduceID: reduceID,
			FileName: fileName,
		}
	}
	return intermediateFileInfo, nil
}

func doReduceTask(reducef func(string, []string) string, task FetchTaskReply) error {
	fileNames := strings.Split(task.FileName, "|")
	intermediate, err := readMapIntermediate(fileNames)
	if err != nil {
		return err
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"
	ofile, _ := os.CreateTemp("./", oname+"-temp")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write reduce out file,intermediate.key:%v,output:%v", intermediate[i].Key, output)
			return err
		}
		i = j
	}

	os.Rename(ofile.Name(), "./"+oname+strconv.Itoa(task.TaskID))
	ofile.Close()
	return nil
}

func readMapIntermediate(fileNames []string) ([]KeyValue, error) {
	kva := []KeyValue{}
	for _, fileName := range fileNames {
		open, err := os.Open(fileName)
		defer open.Close()
		if err != nil {
			log.Fatalf("reduce work cannot open intermediate file %v", fileNames)
			return kva, err
		}

		dec := json.NewDecoder(open)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}

	return kva, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
