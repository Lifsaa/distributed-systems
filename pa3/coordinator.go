/*
coordinator.go manages the MapReduce job: splits inFile,
then hands out map and reduce tasks to workers via RPC,
and detects crashed workers via timeout
*/
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// task states
const (
	TaskIdle       = "idle"
	TaskInProgress = "inprogress"
	TaskDone       = "done"
)

// job phases
const (
	PhaseMap      = "map"
	PhaseReduce   = "reduce"
	PhaseComplete = "complete"
)

// how long before we consider a worker crashed
const TaskTimeout = 5 * time.Minute

// task holds the state of a single map or reduce task
type task struct {
	ID         int
	State      string
	AssignedAt time.Time
	WorkerID   string
	InputFile  string // only used for map tasks
}

type Coordinator struct {
	mu            sync.Mutex // guards all fields
	mapTasks      []task
	reduceTasks   []task
	numMap        int
	numReduce     int
	phase         string
	done          bool
	workerSplits  map[string][]string
	mapWorkerAddr []string
}

// newCoordinator sets up the coordinator by:
// splitting the file into numMap pieces, initialising all task states,
// and starting a goroutine to detect timed-out workers
func newCoordinator(inFile string, numMap, numReduce int) *Coordinator {
	c := &Coordinator{
		numMap:        numMap,
		numReduce:     numReduce,
		phase:         PhaseMap,
		workerSplits:  make(map[string][]string),
		mapWorkerAddr: make([]string, numMap),
	}

	splits := splitFile(inFile, numMap)

	c.mapTasks = make([]task, numMap)
	for i, s := range splits {
		c.mapTasks[i] = task{ID: i, State: TaskIdle, InputFile: s}
	}

	c.reduceTasks = make([]task, numReduce)
	for i := 0; i < numReduce; i++ {
		c.reduceTasks[i] = task{ID: i, State: TaskIdle}
	}

	go c.timeoutLoop()
	return c
}

// Register is called by each worker on startup so coord
// knows its address and which sp;it it owns
func (c *Coordinator) Register(req *RegisterRequest, resp *RegisterResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerSplits[req.WorkerAddr] = req.Splits
	log.Printf("worker registered: %s owns %v", req.WorkerAddr, req.Splits)
	return nil

}

// splitFile splits filename into numParts line-aligned chunk files
// named split-0, split-1, ... streams line by line to avoid loading the whole file
func splitFile(filename string, numParts int) []string {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("splitFile: cannot open %s: %v", filename, err)
	}
	defer f.Close()

	// pass 1: count total lines
	nLines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		nLines++
	}
	if sc.Err() != nil {
		log.Fatalf("splitFile: scan error: %v", sc.Err())
	}

	// rewind for second pass
	if _, err := f.Seek(0, 0); err != nil {
		log.Fatalf("splitFile: seek failed: %v", err)
	}
	linesPerChunk := (nLines + numParts - 1) / numParts

	splits := make([]string, numParts)
	sc = bufio.NewScanner(f)
	for i := 0; i < numParts; i++ {
		name := fmt.Sprintf("split-%d", i)
		splits[i] = name
		out, err := os.Create(name)
		if err != nil {
			log.Fatalf("splitFile: cannot create %s: %v", name, err)
		}
		w := bufio.NewWriter(out)
		for j := 0; j < linesPerChunk && sc.Scan(); j++ {
			w.WriteString(sc.Text())
			w.WriteByte('\n')
		}
		w.Flush()
		out.Close()
	}
	if sc.Err() != nil {
		log.Fatalf("splitFile: scan error: %v", sc.Err())
	}
	return splits
}

// GetTask is called by a worker over RPC when ready for work.
// Assigns the next idle task, or tells the worker to wait or exit.
func (c *Coordinator) GetTask(
	req *GetTaskRequest, resp *GetTaskResponse,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		resp.TaskType = "exit"
		resp.Done = true
		return nil
	}

	switch c.phase {
	case PhaseMap:
		for i := range c.mapTasks {
			if c.mapTasks[i].State == TaskIdle {
				c.mapTasks[i].State = TaskInProgress
				c.mapTasks[i].AssignedAt = time.Now()
				c.mapTasks[i].WorkerID = req.WorkerID
				c.mapWorkerAddr[i] = req.WorkerID + ":" + WorkerPort
				resp.TaskType = "map"
				resp.TaskID = i
				resp.NumReduce = c.numReduce
				resp.InputFile = c.mapTasks[i].InputFile
				return nil
			}
		}
	case PhaseReduce:
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == TaskIdle {
				c.reduceTasks[i].State = TaskInProgress
				c.reduceTasks[i].AssignedAt = time.Now()
				c.reduceTasks[i].WorkerID = req.WorkerID
				resp.TaskType = "reduce"
				resp.TaskID = i
				resp.NumMap = c.numMap
				resp.MapWorkerAddrs = c.mapWorkerAddr
				return nil
			}
		}
	}

	// all tasks are in progress; tell worker to wait
	resp.TaskType = "wait"
	return nil
}

// ReportTask is called by a worker over RPC when it finishes a task.
// On success marks the task done; on failure marks it idle for re-assignment.
func (c *Coordinator) ReportTask(
	req *ReportTaskRequest, resp *ReportTaskResponse,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []task
	if req.TaskType == "map" {
		tasks = c.mapTasks
	} else {
		tasks = c.reduceTasks
	}

	if req.TaskID < 0 || req.TaskID >= len(tasks) {
		return nil
	}

	// ignore stale reports from workers whose task was reassigned
	if tasks[req.TaskID].WorkerID != req.WorkerID {
		return nil
	}

	if req.Success {
		tasks[req.TaskID].State = TaskDone
	} else {
		// worker failure; mark idle so it gets re-assigned
		tasks[req.TaskID].State = TaskIdle
	}

	c.advancePhaseIfReady()
	return nil
}

// advancePhaseIfReady moves the job to the next phase when all
// current tasks are done
func (c *Coordinator) advancePhaseIfReady() {
	if c.phase == PhaseMap && allDone(c.mapTasks) {
		log.Println("All map tasks done, starting reduce phase")
		c.phase = PhaseReduce
	} else if c.phase == PhaseReduce && allDone(c.reduceTasks) {
		log.Println("All reduce tasks done, job complete")
		c.phase = PhaseComplete
		c.done = true
	}
}

// allDone returns true if every task in the slice has state TaskDone
func allDone(tasks []task) bool {
	for _, t := range tasks {
		if t.State != TaskDone {
			return false
		}
	}
	return true
}

// timeoutLoop ticks every second and re-queues timed-out tasks
func (c *Coordinator) timeoutLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.detectTimeoutsOnce()
	}
}

// detectTimeoutsOnce marks any in-progress task that has been running
// longer than TaskTimeout back to idle for re-assignment
func (c *Coordinator) detectTimeoutsOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for i := range c.mapTasks {
		if c.mapTasks[i].State == TaskInProgress &&
			now.Sub(c.mapTasks[i].AssignedAt) > TaskTimeout {
			log.Printf("map task %d timed out, reassigning", i)
			c.mapTasks[i].State = TaskIdle
			c.mapTasks[i].WorkerID = ""
		}
	}
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == TaskInProgress &&
			now.Sub(c.reduceTasks[i].AssignedAt) > TaskTimeout {
			log.Printf("reduce task %d timed out, reassigning", i)
			c.reduceTasks[i].State = TaskIdle
			c.reduceTasks[i].WorkerID = ""
		}
	}
}

// Done returns true once all reduce tasks are complete
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// startRPCServer registers the coordinator and starts accepting
// TCP connections in a background goroutine
func startRPCServer(c *Coordinator) net.Listener {
	rpc.Register(c)
	l, err := net.Listen("tcp", CoordinatorListenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go rpc.Accept(l)
	return l
}
