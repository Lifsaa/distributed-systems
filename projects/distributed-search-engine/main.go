/*
main.go is the entry point for coordinator, worker, and client.
ROLE env var picks which role to run.
*/
package main

import (
	"log"
	"os"
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
		// stay alive forever to serve Search RPCs
		select {}
	case "worker":
		workerMain()
	case "client":
		clientMain()
	default:
		log.Fatal("ROLE must be coordinator, worker, or client")
	}
}
