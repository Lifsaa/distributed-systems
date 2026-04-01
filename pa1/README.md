# PA1 - Web Crawler & Indexer
CPE-469 Distributed Systems

## What it does
Crawls the web starting from a seed URL, builds an inverted index of every word found, and saves everything to files.

## Requirements
- Go 1.21+
- Internet connection

## How to run

```bash
go run main.go
```

## Output files
- `crawler.log` — every URL visited with a timestamp
- `index.json` — inverted index: each word mapped to the list of URLs it appeared on

## Seed URL
Set in `main.go` inside the `seeds` slice. Currently set to `https://books.toscrape.com/`.  
To change it, edit this part:
```go
seeds := []string{
    "https://books.toscrape.com/",
}
```

## Stop words
Loaded from `stopword.txt`, one word per line. These are filtered out before indexing.

## Limits
- Max pages: 10,000,000 (hard cap, stops automatically)
- Stops early if the frontier runs out of new links
