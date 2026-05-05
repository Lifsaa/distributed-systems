/*
crawler.go holds the PA1-style web fetch + HTML walk used by crawl workers:
loadStopWords, cleanText, and crawl (HTTP client with timeout, golang.org/x/net/html).
*/
package main

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"

	"golang.org/x/net/html"
)

// crawlHTTP bounds how long a single page fetch may block the worker
var crawlHTTP = &http.Client{Timeout: time.Duration(readIntEnv("CRAWL_TIMEOUT_SECONDS", 8)) * time.Second}

// loadStopWords reads one stop word per line into a set
func loadStopWords(filename string) (map[string]bool, error) {
	sw := make(map[string]bool)
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	for _, word := range strings.Split(string(data), "\n") {
		word = strings.TrimSpace(word)
		if word != "" {
			sw[word] = true
		}
	}
	return sw, nil
}

// cleanText lowercases, strips punctuation, splits on space, removes stop words
func cleanText(text string, stopWords map[string]bool) []string {
	text = strings.ToLower(text)
	clean := strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) {
			return -1
		}
		return r
	}, text)
	words := strings.Fields(clean)
	var result []string
	for _, word := range words {
		if !stopWords[word] {
			result = append(result, word)
		}
	}
	return result
}

// crawl returns visible text plus absolute http(s) links for one page (empty on failure)
func crawl(pageURL string) (string, []string) {
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		return "", nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; pa4-crawler/1.0)")

	resp, err := crawlHTTP.Do(req)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if ct != "" && !strings.Contains(ct, "text/html") {
		return "", nil
	}

	maxBytes := readIntEnv("CRAWL_MAX_BYTES", 2097152)
	doc, err := html.Parse(io.LimitReader(resp.Body, int64(maxBytes)))
	if err != nil {
		return "", nil
	}

	base, err := url.Parse(pageURL)
	if err != nil {
		return "", nil
	}

	var text strings.Builder
	var links []string

	var traverse func(*html.Node)
	traverse = func(n *html.Node) {
		if n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style") {
			return
		}
		if n.Type == html.TextNode {
			text.WriteString(n.Data + " ")
		}
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					ref, err := url.Parse(attr.Val)
					if err != nil {
						continue
					}
					absolute := base.ResolveReference(ref)
					absolute.Fragment = ""
					if absolute.Scheme == "http" || absolute.Scheme == "https" {
						links = append(links, absolute.String())
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}
	traverse(doc)

	return text.String(), links
}
