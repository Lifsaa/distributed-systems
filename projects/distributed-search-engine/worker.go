/*
worker.go implements the worker loop: register with the coordinator, repeatedly GetTask,
then run crawl / reduce / replicate work and ReportTask. Also serves Worker RPCs so peers
can pull intermediate JSON and mr-out files (same pattern as PA3 map/reduce fetches).
*/
package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// in-memory inverted index loaded once after reduce/replicate completes
var indexMap map[string][]string
var indexMu sync.RWMutex

type Worker struct{}

// dialRPC wraps net.DialTimeout so stuck peers do not block forever
func dialRPC(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// sanitizeMRReadPath keeps FetchFile reads inside /tmp/mr
func sanitizeMRReadPath(path string) (string, error) {
	p := filepath.Clean(path)
	if p != "/tmp/mr" && !strings.HasPrefix(p, "/tmp/mr/") {
		return "", fmt.Errorf("path outside /tmp/mr")
	}
	return p, nil
}

// ihash returns a non-negative hash of key (same trick as PA3)
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Heartbeat is called by the coordinator to see if this worker is still alive
func (w *Worker) Heartbeat(_ *HeartbeatRequest, resp *HeartbeatResponse) error {
	resp.Alive = true
	return nil
}

// FetchFile serves bytes from /tmp/mr for reduce/replicate peers
func (w *Worker) FetchFile(req *FetchFileRequest, resp *FetchFileResponse) error {
	path, err := sanitizeMRReadPath(req.Filename)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	resp.Data = data
	return nil
}

// Lookup serves keyword queries from the in-memory index
func (w *Worker) Lookup(req *LookupRequest, resp *LookupResponse) error {
	indexMu.RLock()
	defer indexMu.RUnlock()
	if indexMap != nil {
		resp.URLs = indexMap[req.Keyword]
	}
	return nil
}

// loadIndex reads mr-out JSON files from /tmp/mr into indexMap
func loadIndex(files []string) error {
	matches := files
	if len(matches) == 0 {
		var err error
		matches, err = filepath.Glob("/tmp/mr/mr-out-*.json")
		if err != nil {
			return err
		}
	}
	m := make(map[string][]string)
	for _, name := range matches {
		path := filepath.Join("/tmp/mr", filepath.Base(name))
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var partial map[string][]string
		if err := json.Unmarshal(data, &partial); err != nil {
			return err
		}
		for word, urls := range partial {
			m[word] = append(m[word], urls...)
		}
	}
	indexMu.Lock()
	indexMap = m
	indexMu.Unlock()
	log.Printf("index loaded: %d words from %d shards", len(m), len(matches))
	return nil
}

// Replicate pulls a shard from another worker and stores it locally (optional RPC path)
func (w *Worker) Replicate(req *ReplicateRequest, resp *ReplicateResponse) error {
	client, err := dialRPC(req.SourceAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	path := filepath.Join("/tmp/mr", filepath.Base(req.Filename))
	freq := &FetchFileRequest{Filename: path}
	fresp := &FetchFileResponse{}
	if err := client.Call("Worker.FetchFile", freq, fresp); err != nil {
		return err
	}
	if err := os.MkdirAll("/tmp/mr", 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, fresp.Data, 0o644); err != nil {
		return err
	}
	resp.Data = nil
	return nil
}

// startWorkerRPCServer listens for peer/coordinator RPCs on WorkerPort
func startWorkerRPCServer(listenAddr string) {
	w := &Worker{}
	srv := rpc.NewServer()
	if err := srv.Register(w); err != nil {
		log.Fatalf("worker rpc register: %v", err)
	}
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("worker listen %s: %v", listenAddr, err)
	}
	go srv.Accept(l)
}

func ensureMRDir() error {
	return os.MkdirAll("/tmp/mr", 0o755)
}

// wordURLs is one piece of the inverted index: word -> pages containing it.
type wordURLs map[string][]string

type crawlResult struct {
	pageURL string
	text    string
	links   []string
}

// linkAllowed keeps discovered links inside the configured crawl host when set.
func linkAllowed(raw, allowedHost string) bool {
	if allowedHost == "" {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Hostname(), allowedHost)
}

/*
crawlBatch fetches each URL, tokenises with cleanText, hashes words to reduce buckets,
and writes intermediate-{task}-{bucket}.json files plus the union of discovered links.
*/
func crawlBatch(taskID int, urls []string, numReduce int, sw map[string]bool) ([]string, error) {
	if err := ensureMRDir(); err != nil {
		return nil, err
	}
	buckets := make([]wordURLs, numReduce)
	for i := range buckets {
		buckets[i] = make(wordURLs)
	}
	var allLinks []string
	allowedHost := strings.TrimSpace(os.Getenv("CRAWL_ALLOWED_HOST"))
	parallelism := readIntEnv("CRAWL_PARALLELISM", 12)
	if parallelism > len(urls) {
		parallelism = len(urls)
	}

	jobs := make(chan string)
	results := make(chan crawlResult)
	var wg sync.WaitGroup

	// Start a small pool of crawler goroutines for this one batch.
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageURL := range jobs {
				text, links := crawl(pageURL)
				results <- crawlResult{pageURL: pageURL, text: text, links: links}
			}
		}()
	}
	go func() {
		// Feed URLs to the goroutines, then close results after all crawls finish.
		for _, pageURL := range urls {
			jobs <- pageURL
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for result := range results {
		for _, link := range result.links {
			if linkAllowed(link, allowedHost) {
				allLinks = append(allLinks, link)
			}
		}
		pageWords := make(map[string]bool)
		words := cleanText(result.text, sw)
		for _, word := range words {
			if pageWords[word] {
				continue
			}
			pageWords[word] = true
			// Shard each word by hash so the same word always goes to the same reducer.
			b := ihash(word) % numReduce
			buckets[b][word] = append(buckets[b][word], result.pageURL)
		}
	}
	for b := 0; b < numReduce; b++ {
		path := fmt.Sprintf("/tmp/mr/intermediate-%d-%d.json", taskID, b)
		data, err := json.Marshal(buckets[b])
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, err
		}
	}
	return dedupeStrings(allLinks), nil
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

/*
reduceBatch pulls every intermediate shard for this reducer, merges the inverted maps,
dedupes URL lists per word, and writes mr-out-{reduce}.json.
*/
func reduceBatch(reduceID int, numCrawl int, crawlAddrs []string) error {
	if err := ensureMRDir(); err != nil {
		return err
	}
	merged := make(wordURLs)
	errs := make([]error, numCrawl)
	for crawlID := 0; crawlID < numCrawl; crawlID++ {
		fn := fmt.Sprintf("/tmp/mr/intermediate-%d-%d.json", crawlID, reduceID)
		// Prefer local files, but fetch from the original crawl worker if needed.
		data, err := os.ReadFile(fn)
		if err == nil {
			var partial wordURLs
			if err := json.Unmarshal(data, &partial); err != nil {
				errs[crawlID] = err
				continue
			}
			for w, urls := range partial {
				merged[w] = append(merged[w], urls...)
			}
			continue
		}
		addr := crawlAddrs[crawlID]
		if addr == "" {
			errs[crawlID] = fmt.Errorf("missing crawl worker for task %d", crawlID)
			continue
		}
		client, err := dialRPC(addr)
		if err != nil {
			errs[crawlID] = err
			continue
		}
		freq := &FetchFileRequest{Filename: fn}
		fresp := &FetchFileResponse{}
		if err := client.Call("Worker.FetchFile", freq, fresp); err != nil {
			errs[crawlID] = err
			client.Close()
			continue
		}
		client.Close()
		var partial wordURLs
		if err := json.Unmarshal(fresp.Data, &partial); err != nil {
			errs[crawlID] = err
			continue
		}
		for w, urls := range partial {
			merged[w] = append(merged[w], urls...)
		}
	}
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	for w, urls := range merged {
		merged[w] = dedupeStrings(urls)
	}
	outPath := fmt.Sprintf("/tmp/mr/mr-out-%d.json", reduceID)
	data, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, data, 0o644)
}

// replicateFile copies one final shard from a peer into our /tmp/mr.
func replicateFile(sourceAddr, filename string) error {
	if err := ensureMRDir(); err != nil {
		return err
	}
	client, err := dialRPC(sourceAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	path := filepath.Join("/tmp/mr", filepath.Base(filename))
	freq := &FetchFileRequest{Filename: path}
	fresp := &FetchFileResponse{}
	if err := client.Call("Worker.FetchFile", freq, fresp); err != nil {
		return err
	}
	return os.WriteFile(path, fresp.Data, 0o644)
}

func coordinatorAddr() string {
	if e := os.Getenv("COORDINATOR_ADDR"); e != "" {
		return e
	}
	return CoordinatorAddr
}

// report sends task completion back to the coordinator (best-effort logging on failure).
func report(client *rpc.Client, workerID, taskType string, taskID int, success bool, newURLs []string) {
	req := &ReportTaskRequest{
		WorkerID: workerID,
		TaskType: taskType,
		TaskID:   taskID,
		Success:  success,
		NewURLs:  newURLs,
	}
	var rr ReportTaskResponse
	if err := client.Call("Coordinator.ReportTask", req, &rr); err != nil {
		log.Printf("ReportTask %s %d: %v", taskType, taskID, err)
	}
}

// runWorker is the main loop: pull a task, do it, report it, repeat.
func runWorker(workerID string, sw map[string]bool) {
	listenHost := "0.0.0.0:" + WorkerPort
	startWorkerRPCServer(listenHost)

	dialCoord := func() (*rpc.Client, error) {
		return dialRPC(coordinatorAddr())
	}
	var coord *rpc.Client
	defer func() {
		if coord != nil {
			coord.Close()
		}
	}()

	reg := &RegisterRequest{WorkerAddr: workerID}
	ensureCoord := func() bool {
		if coord != nil {
			return true
		}
		c2, err := dialCoord()
		if err != nil {
			log.Printf("worker: dial coordinator: %v", err)
			return false
		}
		coord = c2
		if err := coord.Call("Coordinator.Register", reg, &RegisterResponse{}); err != nil {
			log.Printf("worker: register: %v", err)
			coord.Close()
			coord = nil
			return false
		}
		return true
	}

	for {
		if !ensureCoord() {
			time.Sleep(time.Second)
			continue
		}
		req := &GetTaskRequest{WorkerID: workerID}
		resp := &GetTaskResponse{}
		if err := coord.Call("Coordinator.GetTask", req, resp); err != nil {
			log.Printf("worker: GetTask: %v", err)
			coord.Close()
			coord = nil
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch resp.TaskType {
		case "crawl":
			log.Printf("worker %s: crawl task %d (%d URLs)", workerID, resp.TaskID, len(resp.URLs))
			links, err := crawlBatch(resp.TaskID, resp.URLs, resp.NumReduce, sw)
			ok := err == nil
			if err != nil {
				log.Printf("crawl task %d error: %v", resp.TaskID, err)
			}
			report(coord, workerID, "crawl", resp.TaskID, ok, links)

		case "reduce":
			log.Printf("worker %s: reduce task %d", workerID, resp.TaskID)
			err := reduceBatch(resp.TaskID, resp.NumCrawlTasks, resp.CrawlWorkerAddrs)
			ok := err == nil
			if err != nil {
				log.Printf("reduce task %d error: %v", resp.TaskID, err)
			}
			report(coord, workerID, "reduce", resp.TaskID, ok, nil)

		case "replicate":
			log.Printf("worker %s: replicate %s from %s", workerID, resp.Filename, resp.SourceAddr)
			err := replicateFile(resp.SourceAddr, resp.Filename)
			ok := err == nil
			if err != nil {
				log.Printf("replicate error: %v", err)
			}
			report(coord, workerID, "replicate", resp.TaskID, ok, nil)

		case "loadindex":
			log.Printf("worker %s: loadindex", workerID)
			err := loadIndex(resp.Filenames)
			ok := err == nil
			if err != nil {
				log.Printf("loadindex error: %v", err)
			}
			report(coord, workerID, "loadindex", 0, ok, nil)

		case "wait":
			time.Sleep(400 * time.Millisecond)

		case "exit":
			log.Printf("worker %s: exit", workerID)
			return
		default:
			time.Sleep(400 * time.Millisecond)
		}
	}
}

func loadStopPath() string {
	p := os.Getenv("STOPWORD_FILE")
	if p != "" {
		return p
	}
	if _, err := os.Stat("/app/stopword.txt"); err == nil {
		return "/app/stopword.txt"
	}
	return "stopword.txt"
}

// workerMain loads stop words then starts the worker loop for this container
func workerMain() {
	swPath := loadStopPath()
	sw, err := loadStopWords(swPath)
	if err != nil {
		log.Fatalf("stop words: %v", err)
	}
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("hostname: %v", err)
	}
	workerID := host + ":" + WorkerPort
	log.Printf("worker id %s", workerID)
	runWorker(workerID, sw)
}
