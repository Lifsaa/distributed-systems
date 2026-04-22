/*
worker.go implements the worker loop. Each worker repeatedly asks the
coordinator for a task, executes it, and reports the result.
Map phase: read a split file, emit (word, "1") pairs -> write to
intermediate files per reducer bucket.
Reduce phase: fetch intermediate files from map workers over RPC,
count total occurrences per word, and write the sorted result.
*/
package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Worker struct{}

// ihash returns a non-negative hash of key.
// Anding 0x7fffffff is a bitmask to keep the result >= 0.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// FetchFile is called by reduce workers over RPC to get an intermediate
// file from this worker's local filesystem
func (w *Worker) FetchFile(req *FetchFileRequest, resp *FetchFileResponse) error {
	data, err := os.ReadFile(req.Filename)
	if err != nil {
		return err
	}
	resp.Data = data
	return nil
}

// startWorkerRPCServer starts a small RPC server so other workers
// can fetch intermediate files from this worker
func startWorkerRPCServer(addr string) {
	w := &Worker{}
	srv := rpc.NewServer()
	srv.Register(w)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("worker: failed to start RPC server: %v", err)
	}
	go srv.Accept(l)
}

/*
mapTask reads a split file, tokenises it into words, and writes one
intermediate file per reducer bucket.
*/
func mapTask(mapID int, inFile string, numReduce int) error {
	f, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// open one intermediate file per reduce bucket
	writers := make([]*bufio.Writer, numReduce)
	files := make([]*os.File, numReduce)
	for r := 0; r < numReduce; r++ {
		out, err := os.Create(fmt.Sprintf("/tmp/mr/intermediate-%d-%d", mapID, r))
		if err != nil {
			return err
		}
		defer out.Close()
		files[r] = out
		writers[r] = bufio.NewWriter(out)
	}

	// stream input line by line, emit one word per line to the right bucket
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		for _, word := range strings.Fields(sc.Text()) {
			word = strings.ToLower(strings.Trim(word, `.,!?;:"'()-`))
			if word == "" {
				continue
			}
			bucket := ihash(word) % numReduce
			fmt.Fprintln(writers[bucket], word)
		}
	}

	// flush all writers
	for _, w := range writers {
		w.Flush()
	}
	return sc.Err()
}

/*
reduceTask fetches intermediate files from map workers over RPC,
counts total occurrences per word, and writes the sorted result.
*/
func reduceTask(reduceID int, numMap int, mapWorkerAddrs []string) error {
	counts := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for mapID := 0; mapID < numMap; mapID++ {
		wg.Add(1)
		go func(mid int) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", mapWorkerAddrs[mid])
			if err != nil {
				log.Printf("reduce: can't connect to %s: %v", mapWorkerAddrs[mid], err)
				return
			}
			defer client.Close()
			req := &FetchFileRequest{Filename: fmt.Sprintf("/tmp/mr/intermediate-%d-%d", mid, reduceID)}
			resp := &FetchFileResponse{}
			client.Call("Worker.FetchFile", req, resp)
			// plain text: one word per line
			scanner := bufio.NewScanner(strings.NewReader(string(resp.Data)))
			scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
			local := make(map[string]int)
			for scanner.Scan() {
				local[scanner.Text()]++
			}
			mu.Lock()
			for w, c := range local {
				counts[w] += c
			}
			mu.Unlock()
		}(mapID)
	}
	wg.Wait()

	// sort keys so output is deterministic
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.Create(fmt.Sprintf("mr-out-%d", reduceID))
	if err != nil {
		return err
	}
	defer f.Close()

	for _, k := range keys {
		fmt.Fprintf(f, "%s %d\n", k, counts[k])
	}
	return nil
}

// runWorker starts this worker's RPC server, registers with the coordinator,
// then loops asking for tasks until the coordinator says to exit
func runWorker(workerAddr string) {
	startWorkerRPCServer(workerAddr + ":" + WorkerPort)

	coord, err := rpc.Dial("tcp", CoordinatorAddr)
	if err != nil {
		log.Fatalf("worker: can't connect to coordinator: %v", err)
	}
	defer coord.Close()

	regReq := &RegisterRequest{WorkerAddr: workerAddr + ":" + WorkerPort}
	coord.Call("Coordinator.Register", regReq, &RegisterResponse{})

	for {
		req := &GetTaskRequest{WorkerID: workerAddr}
		resp := &GetTaskResponse{}

		if err := coord.Call("Coordinator.GetTask", req, resp); err != nil {
			// wait and retry on RPC error
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch resp.TaskType {
		case "map":
			log.Printf("worker %s: map task %d (%s)", workerAddr, resp.TaskID, resp.InputFile)
			success := mapTask(resp.TaskID, resp.InputFile, resp.NumReduce) == nil
			report(coord, workerAddr, "map", resp.TaskID, success)

		case "reduce":
			log.Printf("worker %s: reduce task %d", workerAddr, resp.TaskID)
			success := reduceTask(resp.TaskID, resp.NumMap, resp.MapWorkerAddrs) == nil
			report(coord, workerAddr, "reduce", resp.TaskID, success)

		case "wait":
			// tasks are in progress; wait and try again
			time.Sleep(500 * time.Millisecond)

		case "exit":
			log.Printf("worker %s: done", workerAddr)
			return
		}
	}
}

/*
report sends the outcome of a finished task to the coordinator.
If the RPC fails, the coordinator will time out and reassign it.
*/
func report(
	client *rpc.Client, workerID, taskType string,
	taskID int, success bool,
) {
	req := &ReportTaskRequest{
		WorkerID: workerID,
		TaskType: taskType,
		TaskID:   taskID,
		Success:  success,
	}
	client.Call("Coordinator.ReportTask", req, &ReportTaskResponse{})
}
