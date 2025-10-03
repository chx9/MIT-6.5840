package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
		taskReply, stop := AskForATask()
		if stop {
			return
		}
		if taskReply.NoTask {
			fmt.Println("No task available, worker waiting...")
			time.Sleep(1 * time.Second)
			continue
		}
		if taskReply.MapOrReduce {
			doMapTask(taskReply.MapTask, mapf)
		} else {
			doReduceTask(taskReply.ReduceTask, reducef)
		}
	}

}

func AskForATask() (AskForATaskReply, bool) {
	stop := false
	args := AskForATaskArgs{}
	reply := AskForATaskReply{}
	ok := call("Coordinator.AskForATask", &args, &reply)
	if ok {
		if reply.AllDone {
			fmt.Println("All tasks are done, worker exiting.")
			stop = true
		}
	} else {
		fmt.Println("AskForATask call failed")
		stop = true
		return reply, stop
	}
	return reply, stop
}
func doMapTask(task *MapTaskType, mapf func(string, string) []KeyValue) {
	// do map
	args := FinishTaskArgs{}
	reply := FinishTaskReply{}
	data, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	content := string(data)
	kvPairs := mapf(task.File, content)
	f, err := os.Create(fmt.Sprintf("mr-map-%d", task.TaskId))
	if err != nil {
		log.Fatalf("cannot create mr-map-%d", task.TaskId)
	}
	defer f.Close()
	for kv := range kvPairs {
		f.WriteString(fmt.Sprintf("%v %v\n", kvPairs[kv].Key, kvPairs[kv].Value))
	}
	args.TaskId = task.TaskId
	args.MapOrReduce = true
	ok := call("Coordinator.FinishATask", &args, &reply)
	if ok {
		fmt.Printf("Map task %d finished successfully.\n", task.TaskId)
	} else {
		fmt.Printf("Failed to finish map task %d.\n", task.TaskId)
	}
}

func doReduceTask(task *ReduceTaskType, reducef func(string, []string) string) {
	args := FinishTaskArgs{}
	reply := FinishTaskReply{}
	args.TaskId = task.TaskId
	args.MapOrReduce = false
	KVMap := make(map[string][]string)
	for i := 0; i < task.FileNumber; i++ {
		fileName := fmt.Sprintf("mr-map-%d", i)
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		var key string
		var value string
		for {
			_, err := fmt.Fscanf(f, "%s %s\n", &key, &value)
			if err != nil {
				break
			}
			if ihash(key)%task.FileNumber == task.TaskId {
				KVMap[key] = append(KVMap[key], value)
			}
		}
	}
	keys := make([]string, 0, len(KVMap))
	for k := range KVMap {
		keys = append(keys, k)
	}
	f, err := os.Create(fmt.Sprintf("mr-out-%d", task.TaskId))
	if err != nil {
		log.Fatalf("cannot create mr-out-%d", task.TaskId)
	}
	sort.Strings(keys)
	for key := range keys {
		reducedValue := reducef(keys[key], KVMap[keys[key]])
		f.WriteString(fmt.Sprintf("%v %v\n", keys[key], reducedValue))
	}
	ok := call("Coordinator.FinishATask", &args, &reply)
	if ok {
		fmt.Printf("Reduce task %d finished successfully.\n", task.TaskId)
	} else {
		fmt.Printf("Failed to finish reduce task %d.\n", task.TaskId)
	}
	
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
