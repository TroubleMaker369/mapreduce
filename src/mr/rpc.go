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
type MapReduceTask struct {
	//need 1.tasktype 2.taskstatus 3.tasknum
	TaskType   string //map,reduce,wait
	TaskStatus string //unassigned,assigned,finished
	TaskNum    int

	//need 1.input of map task    2.input of reduce reduce

	MapFile     string   //input of map task
	ReduceFiles []string //input of reduce reduce

	NumReduce int
	NumMap    int

	TimeStamp int64
}

//rpc Arguments
type MapReduceArgs struct {
	MessageType string //request / finish
	Task        MapReduceTask
}

//rpc return values
type MapReduceReply struct {
	Task MapReduceTask
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
