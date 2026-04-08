/*
worker.go implements the worker loop. Each worker repeatedly asks the
coordinator for a task, executes it, and reports the result.
Map phase: read a split file, emit (word, "1") pairs -> write to
intermediate files per reducer bucket.
Reduce phase: read all intermediate files, count total occurrences
per word, and write the sorted result.
*/
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// KV is a key-value pair written to intermediate files
type KV struct {
	Key   string
	Value string
}

// ihash returns a non-negative hash of key.
// Anding 0x7fffffff is a bitmask to keep the result >= 0.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*
mapTask reads a split file, tokenises it into words, and writes one
intermediate file per reducer bucket.
*/
func mapTask(mapID int, inFile string, numReduce int) error {
	data, err := os.ReadFile(inFile)
	if err != nil {
		return err
	}

	encoders := make([]*json.Encoder, numReduce)
	for r := 0; r < numReduce; r++ {
		f, err := os.Create(fmt.Sprintf("intermediate-%d-%d", mapID, r))
		if err != nil {
			return err
		}
		defer f.Close()
		encoders[r] = json.NewEncoder(f)
	}

	for _, word := range strings.Fields(string(data)) {
		word = strings.ToLower(strings.Trim(word, `.,!?;:"'()-`))
		if word == "" {
			continue
		}
		bucket := ihash(word) % numReduce
		encoders[bucket].Encode(KV{Key: word, Value: "1"})
	}
	return nil
}

/*
reduceTask reads all intermediate files, counts total occurrences per word,
and writes the sorted result.
*/
func reduceTask(reduceID int, numMap int) error {
	counts := make(map[string]int)

	for mapID := 0; mapID < numMap; mapID++ {
		f, err := os.Open(
			fmt.Sprintf("intermediate-%d-%d", mapID, reduceID),
		)
		if err != nil {
			continue // map task produced no words for this bucket
		}
		defer f.Close()

		// read line by line so we don't load the whole file
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var kv KV
			json.Unmarshal(scanner.Bytes(), &kv)
			counts[kv.Key]++
		}
	}

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

// runWorker opens a persistent TCP connection to the coordinator
// and loops asking for tasks until the coordinator says to exit
func runWorker(workerID string) {
	// opening a persistent TCP connection for all RPC calls
	client, err := rpc.Dial("tcp", CoordinatorAddr)
	if err != nil {
		log.Fatalf("worker %s: can't connect to coordinator: %v",
			workerID, err)
	}
	defer client.Close()

	for {
		req := &GetTaskRequest{WorkerID: workerID}
		resp := &GetTaskResponse{}

		if err := client.Call("Coordinator.GetTask", req, resp); err != nil {
			// wait and retry on RPC error
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch resp.TaskType {
		case "map":
			log.Printf("worker %s: map task %d (%s)",
				workerID, resp.TaskID, resp.InputFile)
			success := mapTask(resp.TaskID, resp.InputFile,
				resp.NumReduce) == nil
			report(client, workerID, "map", resp.TaskID, success)

		case "reduce":
			log.Printf("worker %s: reduce task %d", workerID, resp.TaskID)
			success := reduceTask(resp.TaskID, resp.NumMap) == nil
			report(client, workerID, "reduce", resp.TaskID, success)

		case "wait":
			// tasks are in progress; wait and try again
			time.Sleep(500 * time.Millisecond)

		case "exit":
			log.Printf("worker %s: done", workerID)
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
