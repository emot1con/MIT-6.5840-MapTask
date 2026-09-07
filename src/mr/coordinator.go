package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu          sync.Mutex
	Phase       string
	MapTasks    []*Task
	ReduceTasks []*Task
}

type Task struct {
	IDTask    int
	TaskType  string
	State     string
	FileName  string
	TimeStart time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Phase = "map"

	c.MapTasks = make([]*Task, len(files))
	for i, v := range files {
		c.MapTasks[i] = &Task{
			IDTask:   i,
			TaskType: "map",
			State:    "idle",
			FileName: v,
		}
	}

	c.ReduceTasks = make([]*Task, nReduce)
	for i := range nReduce {
		c.ReduceTasks[i] = &Task{
			IDTask:   i,
			TaskType: "reduce",
			State:    "idle",
		}
	}

	c.server(sockname)
	return &c
}

// Helper func to handle timeouts and check if all tasks in a phase are finished
func verifyingTaskPhase(tasks []*Task) bool {
	allDone := true
	for i := range tasks {
		if tasks[i].State == "in_progress" && time.Since(tasks[i].TimeStart) > 10*time.Second {
			tasks[i].State = "idle"
		}
		if tasks[i].State != "completed" {
			allDone = false
		}
	}
	return allDone
}

// Helper func to find and allocate an idle task
func getIdleTask(tasks []*Task) *Task {
	for i := range tasks {
		if tasks[i].State == "idle" {
			tasks[i].State = "in_progress"
			tasks[i].TimeStart = time.Now()
			return tasks[i]
		}
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Phase == "map" && verifyingTaskPhase(c.MapTasks) {
		c.Phase = "reduce"
	}
	if c.Phase == "reduce" && verifyingTaskPhase(c.ReduceTasks) {
		c.Phase = "done"
	}
	if c.Phase == "done" {
		*reply = GetTaskReply{TaskType: "exit"}
		return nil
	}

	if c.Phase == "map" {
		if task := getIdleTask(c.MapTasks); task != nil {
			*reply = GetTaskReply{
				IDTask:   task.IDTask,
				TaskType: task.TaskType,
				MapTask: &MapTask{
					IDMapTask:     task.IDTask,
					FileName:      task.FileName,
					ReduceTaskNum: len(c.ReduceTasks),
				},
			}
			return nil
		}
	}

	if c.Phase == "reduce" {
		if task := getIdleTask(c.ReduceTasks); task != nil {
			*reply = GetTaskReply{
				IDTask:   task.IDTask,
				TaskType: task.TaskType,
				ReduceTask: &ReduceTask{
					IDReduceTask: task.IDTask,
					MapTaskNum:   len(c.MapTasks),
				},
			}
			return nil
		}
	}

	*reply = GetTaskReply{TaskType: "wait"}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		for i := range c.MapTasks {
			if c.MapTasks[i].IDTask == args.IDTask {
				c.MapTasks[i].State = "completed"
				break
			}
		}
	} else if args.TaskType == "reduce" {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].IDTask == args.IDTask {
				c.ReduceTasks[i].State = "completed"
				break
			}
		}
	}

	return nil
}