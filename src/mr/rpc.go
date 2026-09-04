package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type GetTaskArgs struct { }

type GetTaskReply struct {
	IDTask int
	TaskType string
	MapTask *MapTask
	ReduceTask *ReduceTask
}

type ReportTaskArgs struct { 
	IDTask int
	TaskType string
}

type ReportTaskReply struct {
	Status string
}

type MapTask struct {
	IDMapTask int
	FileName string
	ReduceTaskNum int
}

type ReduceTask struct {
	IDReduceTask int
	MapTaskNum int
}