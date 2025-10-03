package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	MapDone       bool
	ReduceDone    bool
	MapDoneNum    int
	ReduceDoneNum int
	Files         []string
	NReduce       int
	MapTasks      []MapTaskType
	ReduceTasks   []ReduceTaskType
	rw            sync.Mutex
	timeout       time.Duration
}
type MapTaskType struct {
	File       string
	FileNumber int
	TaskId     int
	Done       bool
	Doing      bool
	AssignedAt time.Time
}
type ReduceTaskType struct {
	TaskId     int
	FileNumber int
	Done       bool
	Doing      bool
	AssignedAt time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) FinishATask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	fmt.Println("FinishATask called with taskId:", args.TaskId, "MapOrReduce:", args.MapOrReduce)
	if args.MapOrReduce {
		c.MapTasks[args.TaskId].Done = true
		c.MapTasks[args.TaskId].Doing = false
		fmt.Println("Map task", args.TaskId, "done")
		c.MapDoneNum++
		if c.MapDoneNum == len(c.MapTasks) {
			c.MapDone = true
		}
	} else {
		c.ReduceTasks[args.TaskId].Done = true
		c.ReduceTasks[args.TaskId].Doing = false
		fmt.Println("Reduce tas", args.TaskId, "done")
		c.ReduceDoneNum++
		if c.ReduceDoneNum == len(c.ReduceTasks) {
			c.ReduceDone = true
		}
	}
	return nil
}

func (c *Coordinator)MapTaskAssignable(task *MapTaskType) bool{
	timeout := time.Since(task.AssignedAt) > c.timeout
	return !task.Done && (!task.Doing || timeout)
}

func (c *Coordinator)ReduceTaskAssignable(task *ReduceTaskType) bool{
	timeout := time.Since(task.AssignedAt) > c.timeout
	return !task.Done && (!task.Doing || timeout) 
}

func (c *Coordinator) AskForATask(args *AskForATaskArgs, reply *AskForATaskReply) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if !c.MapDone {
		var task *MapTaskType
		var taskId int
		noTask := true
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTaskAssignable(&c.MapTasks[i]) {
				task = &c.MapTasks[i]
				taskId = i
				noTask = false
				break
			}
		}
		fmt.Println("AskForATask called, returning map task:", taskId)
		if task != nil {
			reply.MapTask = task
			reply.MapTask.Doing = true
			reply.MapTask.AssignedAt = time.Now()
		}
		reply.MapOrReduce = true
		reply.AllDone = false
		reply.Timeout = c.timeout
		reply.TaskId = taskId
		reply.NoTask = noTask
	} else if !c.ReduceDone {
		var task *ReduceTaskType
		var taskId int
		noTask := true
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTaskAssignable(&c.ReduceTasks[i]) {
				task = &c.ReduceTasks[i]
				taskId = i
				noTask = false
				break
			}
		}
		fmt.Println("AskForATask called, returning map task:", taskId)
		if task != nil {
			reply.ReduceTask = task
			reply.ReduceTask.Doing = true
			reply.ReduceTask.AssignedAt = time.Now()
		}
		reply.MapOrReduce = false
		reply.AllDone = false
		reply.NoTask = noTask
		reply.TaskId = taskId
	} else {
		reply.AllDone = true
	}
	return nil
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.MapDone && c.ReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce
	c.MapDone = false
	c.ReduceDone = false
	c.MapDoneNum = 0
	c.ReduceDoneNum = 0
	c.MapTasks = make([]MapTaskType, len(files))
	c.ReduceTasks = make([]ReduceTaskType, nReduce)
	c.timeout = time.Duration(10) * time.Second
	for i := 0; i < len(files); i++ {
		c.MapTasks[i].File = files[i]
		c.MapTasks[i].FileNumber = len(files)
		c.MapTasks[i].TaskId = i
		c.MapTasks[i].Done = false
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i].TaskId = i
		c.ReduceTasks[i].FileNumber = len(files)
		c.ReduceTasks[i].Done = false

	}
	c.server()
	return &c
}
