package main

import (
	"os"
	"strings"
	"unicode"
)

var stopWords = make(map[string]bool) // map words to true

func loadStopWords(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	for _, word := range strings.Split(string(data), "\n") {
		word = strings.TrimSpace(word) // for whitespaces
		if word != "" {
			stopWords[word] = true
		}
	}
	return nil
}

func cleanText(text string) []string {
	text = strings.ToLower(text) // unfiformize all to lowercase

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
