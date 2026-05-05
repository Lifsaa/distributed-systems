/*
rpc.go defines the message types and constants that coordinator and workers
use to talk to each other (same layout idea as PA3, extended for crawl/reduce/replicate).
*/
package main

// CoordinatorListenAddr is what the coordinator binds to (all interfaces)
const CoordinatorListenAddr = ":5001"

// CoordinatorAddr is what workers use to dial the coordinator (Docker DNS)
const CoordinatorAddr = "coordinator:5001"

// WorkerPort is the port every worker listens on for worker-to-worker RPC
const WorkerPort = "5002"

// BatchSize is how many URLs the coordinator hands out per crawl task
const BatchSize = 100

// sent by a worker on startup so the coordinator knows its RPC address
type RegisterRequest struct {
	WorkerAddr string
}

// coordinator's ack to a registration
type RegisterResponse struct{}

// sent by a worker when it is ready for work
type GetTaskRequest struct {
	WorkerID string
}

// sent back by the coordinator to tell the worker what to do
// TaskType can be: "crawl", "reduce", "replicate", "loadindex", "wait", or "exit"
type GetTaskResponse struct {
	TaskType         string
	TaskID           int
	URLs             []string
	NumReduce        int
	NumCrawlTasks    int
	CrawlWorkerAddrs []string
	SourceAddr       string
	Filename         string
	Filenames        []string
	Done             bool
}

// sent by a worker when it finishes or fails a task
type ReportTaskRequest struct {
	WorkerID string
	TaskType string
	TaskID   int
	Success  bool
	NewURLs []string // links discovered during crawl (only for crawl tasks)
}

// coordinator's ack to a report
type ReportTaskResponse struct {
	Success bool
}

// coordinator pings the worker; empty body
type HeartbeatRequest struct{}

// worker replies whether it is alive
type HeartbeatResponse struct {
	Alive bool
}

// sent by another worker to read a file under /tmp/mr on this worker
type FetchFileRequest struct {
	Filename string
}

// raw bytes of the requested file
type FetchFileResponse struct {
	Data []byte
}

// optional pull path: dial SourceAddr and fetch Filename into local /tmp/mr
type ReplicateRequest struct {
	SourceAddr string
	Filename   string
}

// unused payload on success; kept for symmetry with the handout RPC sketch
type ReplicateResponse struct {
	Data []byte
}

// client asks the coordinator which worker holds keyword's shard
type SearchRequest struct {
	Keyword string
}

// coordinator returns the worker address to query directly
type SearchResponse struct {
	WorkerAddr string
	Found      bool
}

// client asks a worker to look up a keyword in its in-memory index
type LookupRequest struct {
	Keyword string
}

// worker returns the URL list for the keyword
type LookupResponse struct {
	URLs []string
}
