/*
client.go: runs p threads indefinitely, each sending search queries via
Coordinator.Search (routing) then Worker.Lookup (in-memory index).
Prints avg latency stats every 1000 requests.
*/
package main

import (
	"bufio"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func clientMain() {
	numThreads := readIntEnv("NUM_THREADS", 100)

	// Keywords are the sample search terms the client keeps querying.
	keywords := loadKeywords()
	if len(keywords) == 0 {
		log.Fatal("client: no keywords loaded")
	}

	coordAddr := coordinatorAddr()

	log.Println("client: waiting for search phase...")
	waitForSearchPhase(coordAddr, keywords)
	log.Printf("client: search phase ready, launching %d threads", numThreads)

	var (
		totalRequests int64
		totalNs       int64
		windowNs      int64
		windowCount   int64
		mu            sync.Mutex
	)
	_ = mu

	for i := 0; i < numThreads; i++ {
		go func() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				// Each goroutine acts like a user repeatedly searching one keyword.
				kw := keywords[r.Intn(len(keywords))]
				start := time.Now()
				urls, _ := searchKeyword(coordAddr, kw)
				ns := time.Since(start).Nanoseconds()

				n := atomic.AddInt64(&totalRequests, 1)
				atomic.AddInt64(&totalNs, ns)
				atomic.AddInt64(&windowNs, ns)
				wc := atomic.AddInt64(&windowCount, 1)

				if wc%1000 == 0 {
					avg := atomic.LoadInt64(&windowNs) / 1000 / 1e6
					atomic.StoreInt64(&windowNs, 0)
					log.Printf("client: requests=%d avg_latency_last_1000=%dms keyword_sample=%q urls=%d",
						n, avg, kw, len(urls))
				}
			}
		}()
	}

	// print overall stats every 30s
	for {
		time.Sleep(30 * time.Second)
		n := atomic.LoadInt64(&totalRequests)
		total := atomic.LoadInt64(&totalNs)
		avg := int64(0)
		if n > 0 {
			avg = total / n / 1e6
		}
		log.Printf("client: STATS total_requests=%d overall_avg_latency=%dms", n, avg)
	}
}

// searchKeyword asks the coordinator for the shard holder, then queries that worker.
func searchKeyword(coordAddr, keyword string) ([]string, bool) {
	cc, err := dialRPC(coordAddr)
	if err != nil {
		return nil, false
	}
	defer cc.Close()

	sreq := &SearchRequest{Keyword: keyword}
	sresp := &SearchResponse{}
	if err := cc.Call("Coordinator.Search", sreq, sresp); err != nil || !sresp.Found {
		return nil, false
	}

	wc, err := dialRPC(sresp.WorkerAddr)
	if err != nil {
		return nil, false
	}
	defer wc.Close()

	lreq := &LookupRequest{Keyword: keyword}
	lresp := &LookupResponse{}
	if err := wc.Call("Worker.Lookup", lreq, lresp); err != nil {
		return nil, false
	}
	return lresp.URLs, true
}

// waitForSearchPhase polls until the coordinator can route at least one keyword.
func waitForSearchPhase(coordAddr string, keywords []string) {
	kw := keywords[0]
	for {
		cc, err := dialRPC(coordAddr)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		sreq := &SearchRequest{Keyword: kw}
		sresp := &SearchResponse{}
		err = cc.Call("Coordinator.Search", sreq, sresp)
		cc.Close()
		if err == nil && sresp.Found {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// loadKeywords reads keywords.txt, with KEYWORDS=... as a small fallback.
func loadKeywords() []string {
	path := os.Getenv("KEYWORD_FILE")
	if path == "" {
		path = "/app/keywords.txt"
	}
	f, err := os.Open(path)
	if err != nil {
		raw := os.Getenv("KEYWORDS")
		if raw != "" {
			var out []string
			for _, w := range strings.Split(raw, ",") {
				w = strings.TrimSpace(w)
				if w != "" {
					out = append(out, w)
				}
			}
			return out
		}
		log.Printf("client: cannot open keyword file %s: %v", path, err)
		return nil
	}
	defer f.Close()
	var words []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		w := strings.TrimSpace(sc.Text())
		if w != "" && !strings.HasPrefix(w, "#") {
			words = append(words, w)
		}
	}
	return words
}
