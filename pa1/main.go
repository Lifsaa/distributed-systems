package main

import (
	"net/http"
	"strings"

	"golang.org/x/net/html"
)

// gola is gonna be to fetch a page and return all text content and links found

func crawl(url string) (string, []string) {
	resp, err := http.Get(url)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()

	doc, err := html.Parse(resp.Body) 
	
	if err != nil {
		return "", nil 
	}

	var text strings.Builder
	var links []string

	var traverse func(*html.Node)
	traverse = func(n * html Node) {
		if n.type == html.TextNode (
	
		)
	}


}