package main

import (
	"fmt"
	"net/rpc"
)

// runTester fetches and compares logs from every configured replica.
func runTester(addrs map[string]string) error {
	logs := make(map[string][]LogEntry)
	for name, addr := range addrs {
		entries, err := fetchReplicaLog(addr)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		logs[name] = entries
	}
	if err := compareRaftLogs(logs); err != nil {
		return err
	}
	fmt.Println("PASS logs match")
	return nil
}

// fetchReplicaLog asks one replica for its durable raft log.
func fetchReplicaLog(addr string) ([]LogEntry, error) {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var resp GetLogResponse
	if err := c.Call("Raft.GetLog", &GetLogRequest{}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf(resp.Err)
	}
	return resp.Entries, nil
}

// compareRaftLogs verifies that all replicas have the same entries.
func compareRaftLogs(logs map[string][]LogEntry) error {
	var baseName string
	var base []LogEntry
	for name, entries := range logs {
		baseName = name
		base = entries
		break
	}
	for name, entries := range logs {
		if len(entries) != len(base) {
			return fmt.Errorf("%s has %d entries, %s has %d", name, len(entries), baseName, len(base))
		}
		for i := range base {
			if entries[i] != base[i] {
				return fmt.Errorf("%s entry %d differs: %+v != %+v", name, i+1, entries[i], base[i])
			}
		}
	}
	return nil
}
