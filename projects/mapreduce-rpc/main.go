/*
main.go is the driver: starts the coordinator, spawns W worker
goroutines, and waits until all tasks are done.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"time"
)

func main() {
	// CLI flags - all required, no defaults
	// usage: go run . -input=data.txt -M=9 -R=3 -W=3
	input := flag.String("input", "", "path to the input file")
	M := flag.Int("M", 0, "number of map splits")
	R := flag.Int("R", 0, "number of reduce buckets")
	W := flag.Int("W", 0, "number of worker goroutines")
	flag.Parse()

	if *input == "" || *M == 0 || *R == 0 || *W == 0 {
		log.Fatal(
			"usage: go run . -input=<file> -M=<int> -R=<int> -W=<int>",
		)
	}

	// split input file and set up task queues
	c := newCoordinator(*input, *M, *R)

	// begin accepting worker connections
	l := startRPCServer(c)
	defer l.Close()

	// brief pause so server is ready before workers try to connect
	time.Sleep(50 * time.Millisecond)

	// launch W workers as goroutines to simulate parallel machines
	for i := 0; i < *W; i++ {
		go runWorker(fmt.Sprintf("worker-%d", i))
	}

	// poll until all map and reduce tasks are done
	for !c.Done() {
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Done! Output files: mr-out-0 ... mr-out-%d", *R-1)
}
