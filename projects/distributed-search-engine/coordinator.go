/*
coordinator.go owns the URL frontier, crawl and reduce task state, worker registry,
heartbeats, timeouts, and replication scheduling so each reduce shard has two live copies.
*/
package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func openLogFile() (*os.File, error) {
	os.MkdirAll("/tmp/coord", 0o755)
	return os.OpenFile("/tmp/coord/coordinator.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func coordIhash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// task states (same strings idea as PA3)
const (
	TaskIdle       = "idle"
	TaskInProgress = "inprogress"
	TaskDone       = "done"
)

// job phases for this assignment
const (
	PhaseCrawl    = "crawl"
	PhaseReduce   = "reduce"
	PhaseComplete = "complete"
	PhaseSearch   = "search"
)

// how long an in-flight crawl/reduce can run before we mark it idle again
const (
	CrawlTimeout  = 3 * time.Minute
	ReduceTimeout = 2 * time.Minute
)

// consecutive failed heartbeats before we treat a worker as dead
const HeartbeatMiss = 3

// one crawl batch handed to a worker
type cTask struct {
	ID          int
	State       string
	WorkerID    string
	URLs        []string
	AssignedAt  time.Time
	AssignCount int // >0 means this is a reassignment
}

// one reduce shard task
type rTask struct {
	ID         int
	State      string
	WorkerID   string
	AssignedAt time.Time
}

type workerInfo struct {
	addr          string
	lastHeartbeat time.Time
	missCount     int
	alive         bool
	numReplicas   int
}

type replJob struct {
	sourceAddr string
	filename   string
	reduceID   int
}

type Coordinator struct {
	mu sync.Mutex // guards all fields below

	frontier []string
	visited  map[string]bool

	crawlTasks      []cTask
	crawlWorkerAddr []string
	maxURLs         int
	numReduce       int
	batchSize       int
	startTime       time.Time

	workers map[string]*workerInfo

	reduceTasks []rTask

	replicas map[int][]string

	phase string
	done  bool

	repPending map[string][]replJob

	// search phase
	indexLoadSent map[string]bool
	indexLoaded   map[string]bool

	logFile *os.File
}

// newCoordinator loads seed URLs, starts timeout/heartbeat/replication loops
func newCoordinator(seedFile string, maxURLs, numReduce int) *Coordinator {
	bs := readIntEnv("BATCH_SIZE", BatchSize)
	lf, _ := openLogFile()
	c := &Coordinator{
		visited:       make(map[string]bool),
		workers:       make(map[string]*workerInfo),
		replicas:      make(map[int][]string),
		repPending:    make(map[string][]replJob),
		indexLoadSent: make(map[string]bool),
		indexLoaded:   make(map[string]bool),
		maxURLs:       maxURLs,
		numReduce:     numReduce,
		batchSize:     bs,
		phase:         PhaseCrawl,
		startTime:     time.Now(),
		logFile:       lf,
	}
	data, err := os.ReadFile(seedFile)
	if err != nil {
		log.Fatalf("seed file: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		u := strings.TrimSpace(line)
		if u != "" {
			c.addToFrontier([]string{u})
		}
	}
	go c.timeoutLoop()
	go c.heartbeatLoop()
	go c.replicationLoop()
	return c
}

// addToFrontier keeps the crawl queue growing while skipping URLs we already know.
func (c *Coordinator) addToFrontier(urls []string) {
	for _, u := range urls {
		if c.visited[u] {
			continue
		}
		dup := false
		for _, x := range c.frontier {
			if x == u {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		c.frontier = append(c.frontier, u)
	}
}

// getNextBatch pops up to batchSize URLs and marks them visited before assigning.
func (c *Coordinator) getNextBatch() []string {
	if len(c.visited) >= c.maxURLs {
		return nil
	}
	var batch []string
	for len(batch) < c.batchSize && len(c.frontier) > 0 && len(c.visited) < c.maxURLs {
		u := c.frontier[0]
		c.frontier = c.frontier[1:]
		if c.visited[u] {
			continue
		}
		c.visited[u] = true
		batch = append(batch, u)
	}
	return batch
}

// anyCrawlInProgress tells us if we still need to wait before reducing.
func (c *Coordinator) anyCrawlInProgress() bool {
	for i := range c.crawlTasks {
		if c.crawlTasks[i].State == TaskInProgress {
			return true
		}
	}
	return false
}

// allCrawlDone means every crawl task we created has reported success.
func (c *Coordinator) allCrawlDone() bool {
	if len(c.crawlTasks) == 0 {
		return false
	}
	for i := range c.crawlTasks {
		if c.crawlTasks[i].State != TaskDone {
			return false
		}
	}
	return true
}

// allReduceDone means every reduce shard has been produced at least once.
func (c *Coordinator) allReduceDone() bool {
	if len(c.reduceTasks) == 0 {
		return false
	}
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State != TaskDone {
			return false
		}
	}
	return true
}

// tryAdvanceToReduce moves from crawl/map to reduce once crawling has no more work.
func (c *Coordinator) tryAdvanceToReduce() {
	if c.phase != PhaseCrawl {
		return
	}
	if c.anyCrawlInProgress() {
		return
	}
	if len(c.crawlTasks) == 0 {
		return
	}
	if !c.allCrawlDone() {
		return
	}
	ready := (len(c.frontier) == 0) || (len(c.visited) >= c.maxURLs)
	if !ready {
		return
	}
	if len(c.reduceTasks) == 0 {
		c.reduceTasks = make([]rTask, c.numReduce)
		for i := 0; i < c.numReduce; i++ {
			c.reduceTasks[i] = rTask{ID: i, State: TaskIdle}
		}
	}
	c.logf("crawl done (%d URLs visited) elapsed=%.1fs, starting reduce", len(c.visited), time.Since(c.startTime).Seconds())
	c.phase = PhaseReduce
}

// tryAdvanceToComplete waits for all reduce shards before starting replication checks.
func (c *Coordinator) tryAdvanceToComplete() {
	if c.phase != PhaseReduce {
		return
	}
	if !c.allReduceDone() {
		return
	}
	log.Println("all reduce tasks done")
	c.phase = PhaseComplete
}

// checkGlobalDone enters search only after every shard has two live copies.
func (c *Coordinator) checkGlobalDone() {
	if c.phase != PhaseComplete {
		return
	}
	if !c.allReduceDone() {
		return
	}
	for r := 0; r < c.numReduce; r++ {
		if c.aliveReplicaCount(r) < 2 {
			return
		}
	}
	c.phase = PhaseSearch
	c.logf("PHASE search: replication satisfied, index load starting, total_elapsed=%.1fs", time.Since(c.startTime).Seconds())
}

// aliveReplicaCount counts distinct live workers holding this shard.
func (c *Coordinator) aliveReplicaCount(reduceID int) int {
	addrs := c.replicas[reduceID]
	seen := make(map[string]bool)
	n := 0
	for _, a := range addrs {
		w := c.workers[a]
		if w != nil && w.alive && !seen[a] {
			seen[a] = true
			n++
		}
	}
	return n
}

// addReplicaUnique records a shard holder without double-counting the same worker.
func (c *Coordinator) addReplicaUnique(reduceID int, addr string) {
	for _, x := range c.replicas[reduceID] {
		if x == addr {
			return
		}
	}
	c.replicas[reduceID] = append(c.replicas[reduceID], addr)
}

func (c *Coordinator) logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Print(msg)
	if c.logFile != nil {
		fmt.Fprintln(c.logFile, msg)
	}
}

// workerHasReplicas tells us whether a worker should load any index files.
func (c *Coordinator) workerHasReplicas(addr string) bool {
	for r := 0; r < c.numReduce; r++ {
		for _, a := range c.replicas[r] {
			if a == addr {
				return true
			}
		}
	}
	return false
}

// pickReplicaTarget chooses a live worker that does not already hold the shard.
func (c *Coordinator) pickReplicaTarget(source string, reduceID int) string {
	holders := make(map[string]bool)
	for _, a := range c.replicas[reduceID] {
		holders[a] = true
	}
	best := ""
	bestN := 1 << 30
	for addr, wi := range c.workers {
		if !wi.alive || addr == source || holders[addr] {
			continue
		}
		if wi.numReplicas < bestN {
			bestN = wi.numReplicas
			best = addr
		}
	}
	return best
}

// hasPendingRepl avoids queueing the same shard copy more than once.
func (c *Coordinator) hasPendingRepl(reduceID int) bool {
	for _, jobs := range c.repPending {
		for _, j := range jobs {
			if j.reduceID == reduceID {
				return true
			}
		}
	}
	return false
}

// scheduleReplicaIfNeededLocked queues a copy job until the shard has two live copies.
func (c *Coordinator) scheduleReplicaIfNeededLocked(reduceID int) {
	if c.phase != PhaseComplete && c.phase != PhaseReduce && c.phase != PhaseSearch {
		return
	}
	if reduceID < 0 || reduceID >= c.numReduce {
		return
	}
	if c.reduceTasks[reduceID].State != TaskDone {
		return
	}
	if c.aliveReplicaCount(reduceID) >= 2 {
		return
	}
	if c.hasPendingRepl(reduceID) {
		return
	}
	holders := c.replicas[reduceID]
	var source string
	for _, a := range holders {
		if w := c.workers[a]; w != nil && w.alive {
			source = a
			break
		}
	}
	if source == "" {
		return
	}
	target := c.pickReplicaTarget(source, reduceID)
	if target == "" || target == source {
		return
	}
	for _, j := range c.repPending[target] {
		if j.reduceID == reduceID {
			return
		}
	}
	fn := "mr-out-" + strconv.Itoa(reduceID) + ".json"
	c.repPending[target] = append(c.repPending[target], replJob{
		sourceAddr: source,
		filename:   fn,
		reduceID:   reduceID,
	})
	c.logf("REPLICATE %s ON WORKER %s", fn, target)
}

// Register is called by each worker on startup so the coordinator knows its address
func (c *Coordinator) Register(req *RegisterRequest, resp *RegisterResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	wi, ok := c.workers[req.WorkerAddr]
	if !ok {
		c.workers[req.WorkerAddr] = &workerInfo{
			addr: req.WorkerAddr, alive: true, missCount: 0,
		}
	} else {
		wi.alive = true
		wi.missCount = 0
	}
	log.Printf("worker registered: %s", req.WorkerAddr)
	return nil
}

// GetTask is called by a worker when it is ready for more work
func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tryAdvanceToReduce()

	if jobs, ok := c.repPending[req.WorkerID]; ok && len(jobs) > 0 {
		j := jobs[0]
		c.repPending[req.WorkerID] = jobs[1:]
		if len(c.repPending[req.WorkerID]) == 0 {
			delete(c.repPending, req.WorkerID)
		}
		resp.TaskType = "replicate"
		resp.SourceAddr = j.sourceAddr
		resp.Filename = j.filename
		resp.TaskID = j.reduceID
		return nil
	}

	switch c.phase {
	case PhaseCrawl:
		for i := range c.crawlTasks {
			if c.crawlTasks[i].State == TaskIdle && len(c.crawlTasks[i].URLs) > 0 {
				c.assignCrawl(i, req.WorkerID, resp)
				return nil
			}
		}
		batch := c.getNextBatch()
		if len(batch) > 0 {
			id := len(c.crawlTasks)
			c.crawlTasks = append(c.crawlTasks, cTask{
				ID: id, State: TaskIdle, URLs: batch,
			})
			c.crawlWorkerAddr = append(c.crawlWorkerAddr, "")
			c.assignCrawl(id, req.WorkerID, resp)
			return nil
		}
		if !c.anyCrawlInProgress() && len(c.crawlTasks) > 0 && c.allCrawlDone() {
			c.tryAdvanceToReduce()
			if c.phase == PhaseReduce && c.assignReduceIfIdle(req.WorkerID, resp) {
				return nil
			}
		}
		resp.TaskType = "wait"
		return nil

	case PhaseReduce:
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == TaskIdle {
				c.reduceTasks[i].State = TaskInProgress
				c.reduceTasks[i].AssignedAt = time.Now()
				c.reduceTasks[i].WorkerID = req.WorkerID
				c.logf("ASSIGN REDUCE TASK %d TO WORKER %s", i, req.WorkerID)
				resp.TaskType = "reduce"
				resp.TaskID = i
				resp.NumReduce = c.numReduce
				resp.NumCrawlTasks = len(c.crawlTasks)
				resp.CrawlWorkerAddrs = append([]string(nil), c.crawlWorkerAddr...)
				return nil
			}
		}
		if !c.allReduceDone() {
			resp.TaskType = "wait"
			return nil
		}
		c.tryAdvanceToComplete()
		for r := 0; r < c.numReduce; r++ {
			c.scheduleReplicaIfNeededLocked(r)
		}
		c.checkGlobalDone()
		resp.TaskType = "wait"
		return nil

	case PhaseComplete:
		for r := 0; r < c.numReduce; r++ {
			c.scheduleReplicaIfNeededLocked(r)
		}
		c.checkGlobalDone()
		resp.TaskType = "wait"
		return nil

	case PhaseSearch:
		if !c.indexLoadSent[req.WorkerID] && c.workerHasReplicas(req.WorkerID) {
			c.indexLoadSent[req.WorkerID] = true
			resp.TaskType = "loadindex"
			resp.TaskID = 0
			for r := 0; r < c.numReduce; r++ {
				for _, addr := range c.replicas[r] {
					if addr == req.WorkerID {
						resp.Filenames = append(resp.Filenames, "mr-out-"+strconv.Itoa(r)+".json")
						break
					}
				}
			}
			return nil
		}
		resp.TaskType = "wait"
		return nil
	}

	resp.TaskType = "wait"
	return nil
}

// assignReduceIfIdle hands one idle reduce shard to the requesting worker.
func (c *Coordinator) assignReduceIfIdle(workerID string, resp *GetTaskResponse) bool {
	if len(c.reduceTasks) == 0 {
		return false
	}
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == TaskIdle {
			c.reduceTasks[i].State = TaskInProgress
			c.reduceTasks[i].AssignedAt = time.Now()
			c.reduceTasks[i].WorkerID = workerID
			c.logf("ASSIGN REDUCE TASK %d TO WORKER %s", i, workerID)
			resp.TaskType = "reduce"
			resp.TaskID = i
			resp.NumReduce = c.numReduce
			resp.NumCrawlTasks = len(c.crawlTasks)
			resp.CrawlWorkerAddrs = append([]string(nil), c.crawlWorkerAddr...)
			return true
		}
	}
	return false
}

// assignCrawl fills the RPC response with one crawl/map task.
func (c *Coordinator) assignCrawl(i int, workerID string, resp *GetTaskResponse) {
	t := &c.crawlTasks[i]
	if t.AssignCount > 0 {
		c.logf("REASSIGN MAP TASK %d TO WORKER %s", i, workerID)
	} else {
		c.logf("ASSIGN MAP TASK %d TO WORKER %s", i, workerID)
	}
	t.State = TaskInProgress
	t.AssignedAt = time.Now()
	t.WorkerID = workerID
	t.AssignCount++
	c.crawlWorkerAddr[i] = workerID
	resp.TaskType = "crawl"
	resp.TaskID = i
	resp.URLs = append([]string(nil), t.URLs...)
	resp.NumReduce = c.numReduce
	resp.NumCrawlTasks = len(c.crawlTasks)
}

// ReportTask is called when a worker finishes or fails a crawl/reduce/replicate step.
func (c *Coordinator) ReportTask(req *ReportTaskRequest, resp *ReportTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp.Success = true

	switch req.TaskType {
	case "crawl":
		if req.TaskID < 0 || req.TaskID >= len(c.crawlTasks) {
			return nil
		}
		t := &c.crawlTasks[req.TaskID]
		if t.WorkerID != req.WorkerID {
			return nil
		}
		if req.Success {
			t.State = TaskDone
			c.logf("GET MAP TASK %d RESULT", req.TaskID)
			c.addToFrontier(req.NewURLs)
		} else {
			t.State = TaskIdle
			t.WorkerID = ""
			c.crawlWorkerAddr[req.TaskID] = ""
		}
	case "reduce":
		if req.TaskID < 0 || req.TaskID >= len(c.reduceTasks) {
			return nil
		}
		t := &c.reduceTasks[req.TaskID]
		if t.WorkerID != req.WorkerID {
			return nil
		}
		if req.Success {
			t.State = TaskDone
			c.addReplicaUnique(req.TaskID, req.WorkerID)
			if w := c.workers[req.WorkerID]; w != nil {
				w.numReplicas++
			}
			c.scheduleReplicaIfNeededLocked(req.TaskID)
		} else {
			t.State = TaskIdle
			t.WorkerID = ""
		}
	case "replicate":
		if req.Success {
			if w := c.workers[req.WorkerID]; w != nil {
				w.numReplicas++
			}
			c.addReplicaUnique(req.TaskID, req.WorkerID)
		}
	case "loadindex":
		if req.Success {
			c.indexLoaded[req.WorkerID] = true
			c.logf("INDEX LOADED worker=%s", req.WorkerID)
		} else {
			// allow retry on next GetTask
			c.indexLoadSent[req.WorkerID] = false
		}
		return nil
	default:
		return nil
	}

	c.tryAdvanceToReduce()
	c.tryAdvanceToComplete()
	if c.phase == PhaseComplete {
		for r := 0; r < c.numReduce; r++ {
			c.scheduleReplicaIfNeededLocked(r)
		}
	}
	c.checkGlobalDone()
	return nil
}

// markWorkerDead resets lost work and removes dead shard copies from bookkeeping.
func (c *Coordinator) markWorkerDead(dead string) {
	c.logf("WORKER DEAD addr=%s", dead)
	delete(c.indexLoaded, dead)
	delete(c.indexLoadSent, dead)
	wi := c.workers[dead]
	if wi != nil {
		wi.alive = false
	}
	resetCrawl := c.resetLostCrawlTasks(dead)
	if resetCrawl {
		log.Printf("crawl data lost on %s — resetting reduce and crawl assignments", dead)
		c.phase = PhaseCrawl
		for i := range c.reduceTasks {
			c.reduceTasks[i].State = TaskIdle
			c.reduceTasks[i].WorkerID = ""
		}
		c.replicas = make(map[int][]string)
		c.repPending = make(map[string][]replJob)
		for _, w := range c.workers {
			if w != nil {
				w.numReplicas = 0
			}
		}
	} else {
		for rid := range c.replicas {
			var lst []string
			for _, a := range c.replicas[rid] {
				if a != dead {
					lst = append(lst, a)
				}
			}
			c.replicas[rid] = lst
		}
		for i := range c.reduceTasks {
			rt := &c.reduceTasks[i]
			if rt.WorkerID != dead {
				continue
			}
			if rt.State == TaskInProgress {
				rt.State = TaskIdle
				rt.WorkerID = ""
				continue
			}
			if rt.State == TaskDone {
				alive := 0
				for _, a := range c.replicas[i] {
					if w := c.workers[a]; w != nil && w.alive {
						alive++
					}
				}
				if alive == 0 {
					rt.State = TaskIdle
				}
				rt.WorkerID = ""
			}
		}
	}
	for target, jobs := range c.repPending {
		var kept []replJob
		for _, j := range jobs {
			if j.sourceAddr != dead && target != dead {
				kept = append(kept, j)
			}
		}
		if len(kept) == 0 {
			delete(c.repPending, target)
		} else {
			c.repPending[target] = kept
		}
	}
	if !c.allReduceDone() {
		if c.phase == PhaseComplete || c.phase == PhaseSearch {
			c.phase = PhaseReduce
			c.repPending = make(map[string][]replJob)
		}
	}
	if c.phase == PhaseReduce || c.phase == PhaseComplete || c.phase == PhaseSearch {
		for rid := 0; rid < c.numReduce; rid++ {
			if rid < len(c.reduceTasks) && c.reduceTasks[rid].State == TaskDone && c.aliveReplicaCount(rid) < 2 {
				c.scheduleReplicaIfNeededLocked(rid)
			}
		}
	}
}

// resetLostCrawlTasks redoes map work whose intermediate files lived on dead.
func (c *Coordinator) resetLostCrawlTasks(dead string) bool {
	if c.phase != PhaseCrawl && c.phase != PhaseReduce {
		return false
	}
	resetCrawl := false
	for i := range c.crawlTasks {
		t := &c.crawlTasks[i]
		if t.WorkerID != dead && (i >= len(c.crawlWorkerAddr) || c.crawlWorkerAddr[i] != dead) {
			continue
		}
		if t.State == TaskDone || t.State == TaskInProgress {
			resetCrawl = true
		}
		t.State = TaskIdle
		t.WorkerID = ""
		if i < len(c.crawlWorkerAddr) {
			c.crawlWorkerAddr[i] = ""
		}
	}
	return resetCrawl
}

// workerHeartbeatFailed counts misses; after enough misses the worker is dead.
func (c *Coordinator) workerHeartbeatFailed(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.workers[addr]
	if w == nil || !w.alive {
		return
	}
	c.logf("HEARTBEAT FAILED FROM WORKER %s", addr)
	w.missCount++
	if w.missCount >= HeartbeatMiss {
		c.markWorkerDead(addr)
	}
}

// workerHeartbeatOK clears the miss counter for a worker that answered.
func (c *Coordinator) workerHeartbeatOK(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.workers[addr]
	if w == nil {
		return
	}
	w.missCount = 0
	w.lastHeartbeat = time.Now()
}

// heartbeatLoop periodically dials each worker's Heartbeat RPC
func (c *Coordinator) heartbeatLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		addrs := make([]string, 0, len(c.workers))
		for a, w := range c.workers {
			if w.alive {
				addrs = append(addrs, a)
			}
		}
		c.mu.Unlock()
		for _, addr := range addrs {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				c.workerHeartbeatFailed(addr)
				continue
			}
			client := rpc.NewClient(conn)
			var hb HeartbeatResponse
			err = client.Call("Worker.Heartbeat", &HeartbeatRequest{}, &hb)
			client.Close()
			if err != nil || !hb.Alive {
				c.workerHeartbeatFailed(addr)
			} else {
				c.workerHeartbeatOK(addr)
			}
		}
	}
}

// timeoutLoop re-queues crawl/reduce tasks that have been running too long.
func (c *Coordinator) timeoutLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		c.mu.Lock()
		for i := range c.crawlTasks {
			tk := &c.crawlTasks[i]
			if tk.State == TaskInProgress && now.Sub(tk.AssignedAt) > CrawlTimeout {
				log.Printf("crawl task %d timed out", i)
				tk.State = TaskIdle
				tk.WorkerID = ""
				if i < len(c.crawlWorkerAddr) {
					c.crawlWorkerAddr[i] = ""
				}
			}
		}
		for i := range c.reduceTasks {
			tk := &c.reduceTasks[i]
			if tk.State == TaskInProgress && now.Sub(tk.AssignedAt) > ReduceTimeout {
				log.Printf("reduce task %d timed out", i)
				tk.State = TaskIdle
				tk.WorkerID = ""
			}
		}
		c.mu.Unlock()
	}
}

// replicationLoop nudges replica counts toward two live copies per reduce shard.
func (c *Coordinator) replicationLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		if c.phase == PhaseComplete || c.phase == PhaseReduce || c.phase == PhaseSearch {
			for r := 0; r < c.numReduce; r++ {
				c.scheduleReplicaIfNeededLocked(r)
			}
			c.checkGlobalDone()
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// Search is called by clients; it routes a keyword to a worker with that shard loaded.
func (c *Coordinator) Search(req *SearchRequest, resp *SearchResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != PhaseSearch {
		resp.Found = false
		return nil
	}
	reduceID := coordIhash(req.Keyword) % c.numReduce
	for _, addr := range c.replicas[reduceID] {
		if w := c.workers[addr]; w != nil && w.alive && c.indexLoaded[addr] {
			resp.WorkerAddr = addr
			resp.Found = true
			c.logf("ASSIGN SEARCH %s TO WORKER %s", req.Keyword, addr)
			return nil
		}
	}
	resp.Found = false
	return nil
}

// startRPCServer registers the coordinator and listens for worker RPCs
func startRPCServer(c *Coordinator) net.Listener {
	rpc.Register(c)
	l, err := net.Listen("tcp", CoordinatorListenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go rpc.Accept(l)
	return l
}

func readIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n := 0
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return def
		}
		n = n*10 + int(ch-'0')
	}
	if n <= 0 {
		return def
	}
	return n
}

func loadSeedPath() string {
	p := os.Getenv("SEED_FILE")
	if p == "" {
		return "seed-urls.txt"
	}
	return p
}
