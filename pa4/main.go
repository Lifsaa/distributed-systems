/*
main.go is the entry point for both coordinator and worker.
ROLE env var picks which role to run; coordinator reads SEED_FILE / MAX_URLS / NUM_REDUCE.
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
		seed := loadSeedPath()
		max := readIntEnv("MAX_URLS", 10000)
		nr := readIntEnv("NUM_REDUCE", 3)
		c := newCoordinator(seed, max, nr)
		l := startRPCServer(c)
		defer l.Close()
		log.Printf("coordinator listening on %s", CoordinatorListenAddr)
		for !c.Done() {
			time.Sleep(100 * time.Millisecond)
		}
		log.Println("coordinator shutting down")
	case "worker":
		workerMain()
	default:
		log.Fatal("ROLE must be coordinator or worker")
	}
}
