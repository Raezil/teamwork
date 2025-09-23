package helpers

import (
	"regexp"
	"strings"
	"sync"
)

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

func IsEmail(s string) bool {
	return emailRegex.MatchString(s)
}

// worker reads email lines from jobs, extracts domain, and sends results to results channel
func Worker(jobs <-chan []string, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for line := range jobs {
		if len(line) < 3 {
			continue
		}

		addr := strings.TrimSpace(line[2])
		// validate full address email
		if !IsEmail(addr) {
			continue
		}

		_, domain, found := strings.Cut(addr, "@")
		if !found || domain == "" {
			continue
		}

		results <- strings.ToLower(domain)
	}
}
