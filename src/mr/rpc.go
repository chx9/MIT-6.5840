package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// example to show how to declare the arguments
// and reply for an RPC.
type AskForATaskArgs struct {
}
type AskForATaskReply struct {
	MapOrReduce bool
	MapTask     *MapTaskType
	ReduceTask  *ReduceTaskType
	TaskId      int
	AllDone     bool
	Wait        bool
	NoTask      bool
	Timeout     time.Duration
}

type FinishTaskArgs struct {
	MapOrReduce bool
	TaskId      int
}
type FinishTaskReply struct {
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
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
