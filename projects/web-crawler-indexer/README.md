# Web Crawler and Inverted Index

Class step: PA1 for CPE-469 Distributed Systems.

## What it is

This is a small Go web crawler. It starts from a seed URL, follows links, filters stop words, and writes an inverted index from each word to the URLs where it appeared.

## What I worked on

- Keeping a frontier of URLs to visit.
- Avoiding duplicate pages.
- Pulling words and links out of HTML pages.
- Writing the crawl log and index to files.
- Adding a hard page limit so the crawler does not run forever.

## How to run

```bash
go run .
```

The seed URL is in `main.go` in the `seeds` slice. The default is `https://books.toscrape.com/`.

## Checks

```bash
go test ./...
```

There are no unit tests for this one, so the check is mainly a compile check. Running it manually creates `crawler.log` and `index.json`.

## Files to look at

- `main.go`: crawler loop, link parsing, word indexing, and file output.
- `stopword.txt`: words filtered out before indexing.
