package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type FetchTask struct {
}

type FetchTaskReply struct {
	TaskType TaskTypeEnum
	TaskID   int
	FileName string
	NReduce  int
}

type ReportMapTaskDoneArgs struct {
	MapTaskID            int
	IntermediateFileInfo []IntermediateFileInfo
	FileName             string
}

type ReportMapTaskDoneReply struct{}

type ReportReduceTaskDoneArgs struct{
	ReduceTaskID int
}

type ReportReduceTaskDoneReply struct{}

type IntermediateFileInfo struct {
	ReduceID int
	FileName string
}

// Cook up a unique-ish UNIX-domain socet name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
