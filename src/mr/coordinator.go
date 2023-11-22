package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskTypeEnum int

const (
	InitialType TaskTypeEnum = iota
	MapTaskType
	ReduceTaskType
)

const workerTimeout = 6 * time.Second

type Task struct {
	TaskType    TaskTypeEnum
	TaskID      int
	FileName    string
	StartAtTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	mapTaskNum       int
	mapTasks         map[string]Task //map[fileName]*Task
	idleMapTask      []Task
	executingMapTask map[string]Task //map[fileName]*Task
	finishedMapTask  []Task

	reduceTaskNum       int
	reduceTasks         map[string]Task
	idleReduceTask      []Task       //map[reduceIDTaskID]*Task
	executingReduceTask map[int]Task //map[reduceTaskID]*Task
	finishedReduceTask  []Task

	intermediateKvLocList [][]string //record intermediate kv loc
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) FetchTask(args *FetchTask, reply *FetchTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.idleMapTask) > 0 {
		task := c.idleMapTask[0]
		c.idleMapTask = c.idleMapTask[1:]
		task.StartAtTime = time.Now()
		reply.TaskType = MapTaskType
		reply.TaskID = task.TaskID
		reply.FileName = task.FileName
		reply.NReduce = c.reduceTaskNum
		c.executingMapTask[task.FileName] = task
		return nil
	} else if len(c.executingMapTask) > 0 {
		return nil
	} else if len(c.idleReduceTask) > 0 {
		//get reduceTaskID from c.intermediateKvLocList
		//get the first idle reduce task
		//for reduceTaskID, fileName := range c.intermediateKvLocList {
		//	_, ok := c.executingReduceTask[reduceTaskID]
		//	if !ok {
		//		task := c.idleReduceTask[reduceTaskID]
		//		delete(c.idleReduceTask, reduceTaskID)
		//		task.StartAtTime = time.Now()
		//		reply.TaskType = ReduceTaskType
		//		reply.TaskID = reduceTaskID
		//		reply.FileName = strings.Join(fileName, "|")
		//		reply.NReduce = c.reduceTaskNum
		//		c.executingReduceTask[reduceTaskID] = task
		//		return nil
		//	}
		//}
		task := c.idleReduceTask[0]
		c.idleReduceTask = c.idleReduceTask[1:]
		task.StartAtTime = time.Now()
		reply.TaskType = ReduceTaskType
		reply.TaskID = task.TaskID
		reply.FileName = strings.Join(c.intermediateKvLocList[task.TaskID], "|")
		reply.NReduce = c.reduceTaskNum
		c.executingReduceTask[task.TaskID] = task
		return nil
	}
	return nil
}

func (c *Coordinator) ReportMapTaskDone(args *ReportMapTaskDoneArgs, reply *ReportMapTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.executingMapTask[args.FileName]
	if !ok {
		log.Fatalf("report map task done:mapTask id is not executing")
		return nil
	}

	if task.TaskID != args.MapTaskID {
		log.Fatalf("report map task done:mapTask id is not match")
		return nil
	}

	delete(c.executingMapTask, args.FileName)
	c.finishedMapTask = append(c.finishedMapTask, task)
	for _, intermediateFileInfo := range args.IntermediateFileInfo {
		reduceID := intermediateFileInfo.ReduceID
		c.intermediateKvLocList[reduceID] = append(c.intermediateKvLocList[reduceID], intermediateFileInfo.FileName)
	}
	return nil
}

func (c *Coordinator) ReportReduceTaskDone(args *ReportReduceTaskDoneArgs, reply *ReportReduceTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.executingReduceTask[args.ReduceTaskID]
	if !ok {
		log.Fatalf("report reduce task done:reduceTask id is not executing")
		return nil
	}
	delete(c.executingReduceTask, args.ReduceTaskID)
	c.finishedReduceTask = append(c.finishedReduceTask, task)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) checkWorkTimeout() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		for fileName, task := range c.executingMapTask {
			if time.Since(task.StartAtTime) > workerTimeout {
				c.idleMapTask = append(c.idleMapTask, task)
				delete(c.executingMapTask, fileName)
			}
		}

		for _, task := range c.executingReduceTask {
			if time.Since(task.StartAtTime) > workerTimeout {
				c.idleReduceTask = append(c.idleReduceTask, task)
				delete(c.executingReduceTask, task.TaskID)
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go c.checkWorkTimeout()
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	if len(c.finishedMapTask) == c.mapTaskNum && len(c.finishedReduceTask) == c.reduceTaskNum {
		ret = true
	}
	//==============
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskNum:            len(files),
		mapTasks:              make(map[string]Task, len(files)),
		idleMapTask:           make([]Task, 0),
		executingMapTask:      make(map[string]Task, nReduce),
		finishedMapTask:       make([]Task, 0),
		reduceTaskNum:         nReduce,
		reduceTasks:           make(map[string]Task, nReduce),
		idleReduceTask:        make([]Task, nReduce),
		executingReduceTask:   make(map[int]Task, nReduce),
		finishedReduceTask:    make([]Task, 0),
		intermediateKvLocList: make([][]string, nReduce),
	}

	// Your code here.
	for k, v := range files {
		task := Task{
			TaskType: MapTaskType,
			TaskID:   k,
			FileName: v,
		}
		c.mapTasks[v] = task
		c.idleMapTask = append(c.idleMapTask, task)
	}

	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskType: ReduceTaskType,
			TaskID:   i,
		}
		c.reduceTasks[strconv.Itoa(i)] = task
		c.idleReduceTask[i] = task
	}

	c.server()
	return &c
}
