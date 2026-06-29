/*
rpc.go defines the message types that coordinator and workers
use to talk to each other
*/

package main

// CoordinatorListenAddr is what the coordinator binds to (all interfaces)
const CoordinatorListenAddr = ":5001"

// CoordinatorAddr is what workers use to dial the coordinator (Docker DNS)
const CoordinatorAddr = "coordinator:5001"

// WorkerPort is the port every worker listens on for file fetch requests
const WorkerPort = "5002"

// sent by a worker when it is ready for work
type GetTaskRequest struct {
	WorkerID string
}

// sent back by the coordinator to tell the worker what to do
// TaskType can be: "map", "reduce", "wait", or "exit"
// MapWorkerAddrs is only filled for reduce tasks: index i = address of the worker that ran map task i
type GetTaskResponse struct {
	TaskType       string
	TaskID         int
	NumReduce      int
	NumMap         int
	InputFile      string
	Done           bool
	MapWorkerAddrs []string
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

// sent by a worker on startup so the coordinator knows its address & splits
type RegisterRequest struct {
	WorkerAddr string
	Splits     []string
}

// coordinator's ack to a registration
type RegisterResponse struct{}

// sent by a reduce worker to a map worker to fetch an intermediate file
type FetchFileRequest struct {
	Filename string
}

// map worker's reply: the raw bytes of the requested file
type FetchFileResponse struct {
	Data []byte
}
