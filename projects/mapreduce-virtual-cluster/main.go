/*
main.go is the entry point for both coordinator and worker
ROLE env var decides which one to run
*/
package main

import (
	"log"
	"os"
	"time"
)

func main() {
	role := os.Getenv("ROLE")

	switch role {
	case "coordinator":
		input := os.Getenv("INPUT_FILE")
		if input == "" {
			log.Fatal("INPUT_FILE env var is required for coordinator")
		}
		c := newCoordinator(input, 3, 3)
		l := startRPCServer(c)
		defer l.Close()
		for !c.Done() {
			time.Sleep(100 * time.Millisecond)
		}
		log.Printf("Done! Output files: mr-out-0 ... mr-out-%d", 2)
	case "worker":
		addr, _ := os.Hostname()
		runWorker(addr)

	default:
		log.Fatal("ROLE env var must be set to either 'coordinator' or 'worker'")
	}
}
