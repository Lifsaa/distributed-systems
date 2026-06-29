/*
rpc.go defines the message types that coordinator and workers
use to talk to each other
*/

package main

// CoordinatorAddr is the TCP address the coordinator listens on
const CoordinatorAddr = "localhost:5001"

// sent by a worker when it is ready for work
type GetTaskRequest struct {
	WorkerID string
}

// sent back by the coordinator to tell the worker what to do
// TaskType can be: "map", "reduce", "wait", or "exit"
type GetTaskResponse struct {
	TaskType  string
	TaskID    int
	NumReduce int
	NumMap    int
	InputFile string
	Done      bool
}

// sent by a worker when it finishes or fails a task
type ReportTaskRequest struct {
	WorkerID string
	TaskType string
	TaskID   int
	Success  bool
}

// coordinator's ack to a report
type ReportTaskResponse struct {
	Success bool
}
