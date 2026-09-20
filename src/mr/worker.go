package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		reply, ok := getTask()
		if !ok {
			return
		}

		switch reply.TaskType {
		case "exit":
			return
		case "wait":
			time.Sleep(1 * time.Second)
			continue
		case "map":
			if err := doMapTask(reply.MapTask, mapf); err != nil {
				fmt.Printf("Map task failed: %v\n", err)
				continue
			}
			if _, ok := reportTask(reply.IDTask, "map"); !ok {
				fmt.Printf("Recuce Task with ID: %v is fail", reply.TaskType)
				continue
			}

		case "reduce":
			if err := doReduceTask(reply.ReduceTask, reducef); err != nil {
				fmt.Printf("Reduce task failed: %v\n", err)
				continue
			}

			if _, ok := reportTask(reply.IDTask, "reduce"); !ok {
				fmt.Printf("Reduce Task with ID: %v is fail", reply.TaskType)
				continue
			}
		default:
			fmt.Println("Theres Something Wrong, Trying Again")
			continue
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func getTask() (*GetTaskReply, bool) {
	args := new(GetTaskArgs)
	reply := new(GetTaskReply)
	return reply, call("Coordinator.GetTask", &args, &reply)
}

func reportTask(id int, taskType string) (*ReportTaskReply, bool) {
	reportArgs := new(ReportTaskArgs)
	reportArgs.IDTask = id
	reportArgs.TaskType = taskType

	reportReply := new(ReportTaskReply)

	return reportReply, call("Coordinator.ReportTask", &reportArgs, &reportReply)
}

func doMapTask(task *MapTask, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		fmt.Printf("File %s with Map Task ID: %v is fail to open", task.FileName, task.IDMapTask)
		return err
	}
	kva := mapf(task.FileName, string(content))
	buckets := make([][]KeyValue, task.ReduceTaskNum)

	for i := range kva {
		r := ihash(kva[i].Key) % task.ReduceTaskNum
		buckets[r] = append(buckets[r], kva[i])
	}

	for i := range buckets {
		tmpFile, err := os.CreateTemp(".", "mr-temp-*")
		if err != nil {
			fmt.Printf("File %s with Map Task ID: %v is fail to open", task.FileName, task.IDMapTask)
			return err
		}
		enc := json.NewEncoder(tmpFile)

		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return err
			}
		}
		tmpFile.Close()

		// 3. Rename ke nama file final: mr-IDMapTask-i
		oname := fmt.Sprintf("mr-%d-%d", task.IDMapTask, i)
		if err := os.Rename(tmpFile.Name(), oname); err != nil {
			return err
		}
	}
	return nil
}

func doReduceTask(task *ReduceTask, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	for m := 0; m < task.MapTaskNum; m++ {
		filename := fmt.Sprintf("mr-%v-%v", m, task.IDReduceTask)
		file, err := os.Open(filename) 
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.IDReduceTask)
	tempFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		return err
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	return os.Rename(tempFile.Name(), oname)
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
