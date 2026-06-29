package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"

	"golang.org/x/net/html"
)

var stopWords = make(map[string]bool)

func loadStopWords(filename string) error {
	/*
		reads stop words from a text file  into the stopWords map
	*/
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// strings.Split gives us a slice (like list in python), range iterates it
	for _, word := range strings.Split(string(data), "\n") {
		// TrimSpace removes whitespace and \r from both ends
		word = strings.TrimSpace(word)
		if word != "" {
			stopWords[word] = true
		}
	}
	return nil
}

func cleanText(text string) []string {
	/*
		takes raw page text, returns cleaned and filtered words
		removes punctuation, lowercases everything, strips stop words
	*/
	text = strings.ToLower(text)

	// strings.Map applies a function to every character, returning -1 removes it
	clean := strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) {
			return -1
		}
		return r
	}, text)

	// strings.Fields splits on any whitespace, like split() in python
	words := strings.Fields(clean)

	var result []string
	for _, word := range words {
		if !stopWords[word] {
			result = append(result, word)
		}
	}
	return result
}

func crawl(pageURL string) (string, []string) {
	/*
		fetches a page and returns all visible text + all links found on it
		returns empty if the request fails or status is not 2xx
	*/

	// using NewRequest instead of http.Get so we can set headers
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		return "", nil
	}

	// this makes us look like a browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; pa1-crawler/1.0)")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil
	}
	// defer runs when the function exits
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil
	}

	// html.Parse builds a node tree from the response body (like a DOM)
	doc, err := html.Parse(resp.Body)
	if err != nil {
		return "", nil
	}

	// parse the current page URL so we can resolve relative links l
	base, err := url.Parse(pageURL)
	if err != nil {
		return "", nil
	}

	// strings.Builder seems pretty efficient to build up a string piece by piece
	var text strings.Builder
	var links []string

	// closure for traversal
	var traverse func(*html.Node)
	traverse = func(n *html.Node) {
		// skip script and style tags, their content is code not readable text
		if n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style") {
			return
		}

		// TextNode is the actual visible text between tags
		if n.Type == html.TextNode {
			text.WriteString(n.Data + " ")
		}

		// grab href from anchor tags to discover new links
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					ref, err := url.Parse(attr.Val)
					if err != nil {
						continue
					}
					// ResolveReference handles relative URLs, turning /page into https://site.com/page
					absolute := base.ResolveReference(ref)
					absolute.Fragment = "" // strip #section so we don't revisit same page
					if absolute.Scheme == "http" || absolute.Scheme == "https" {
						links = append(links, absolute.String())
					}
				}
			}
		}

		// walk through all children then siblings
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}
	traverse(doc)

	// .String() finalizes the builder into a plain string
	return text.String(), links
}

func addToIndex(words []string, pageURL string, index map[string][]string) {
	/*
		adds each word -> pageURL pair into the inverted index
		if the word is new, append handles the nil slice automatically
	*/
	for _, word := range words {
		index[word] = append(index[word], pageURL)
	}
}

func saveIndex(index map[string][]string, filename string) error {
	/*
		serializes the index map to a pretty-printed JSON file
	*/
	// MarshalIndent converts a Go value to JSON bytes with indentation
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	// 0644 = file permission: owner can read/write, everyone else can only read
	return os.WriteFile(filename, data, 0644)
}

func main() {
	err := loadStopWords("stopword.txt")
	if err != nil {
		fmt.Println("Error loading stop words:", err)
		os.Exit(1)
	}

	logFile, err := os.Create("crawler.log")
	if err != nil {
		fmt.Println("Error creating log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()

	// log.New creates a logger writing to our file, LstdFlags adds date+time to each line
	logger := log.New(logFile, "", log.LstdFlags)

	seeds := []string{
		"https://en.wikipedia.org/wiki/Pizza",
	}

	frontier := seeds
	visited := make(map[string]bool)
	index := make(map[string][]string)
	count := 0
	maxPages := 10000

	// time.Now records the start so we can measure total duration at the end
	start := time.Now()

	// BFS loop: dequeue a URL, crawl it, add new links to the back of the frontier
	for len(frontier) > 0 && count < maxPages {
		pageURL := frontier[0]
		frontier = frontier[1:]

		if visited[pageURL] {
			continue
		}

		visited[pageURL] = true
		count++
		logger.Println(pageURL)
		fmt.Printf("Crawling [%d]: %s\n", count, pageURL)

		text, links := crawl(pageURL)
		words := cleanText(text)
		addToIndex(words, pageURL, index)

		for _, link := range links {
			if !visited[link] {
				frontier = append(frontier, link)
			}
		}
	}

	// time.Since gives us the elapsed time since start
	elapsed := time.Since(start)
	fmt.Printf("\nDone! Crawled %d pages in %s\n", count, elapsed)

	err = saveIndex(index, "index.json")
	if err != nil {
		fmt.Println("Error saving index:", err)
	}

	// map iteration in Go is intentionally random, so these 10 will vary each run
	fmt.Println("\nFirst 10 keywords in index:")
	printed := 0
	for word, urls := range index {
		if printed >= 10 {
			break
		}
		fmt.Printf("  %-20s -> found in %d page(s)\n", word, len(urls))
		printed++
	}
}
