// Package customerimporter reads from a CSV file and returns a sorted (data
// structure of your choice) of email domains along with the number of customers
// with e-mail addresses for each domain. This should be able to be ran from the
// CLI and output the sorted domains to the terminal or to a file. Any errors
// should be logged (or handled). Performance matters (this is only ~3k lines,
// but could be 1m lines or run on a small machine).
package customerimporter

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

type DomainData struct {
	Domain           string
	CustomerQuantity uint64
}

type CustomerImporter struct {
	path *string
}

// NewCustomerImporter returns a new CustomerImporter that reads from file at specified path.
func NewCustomerImporter(filePath *string) *CustomerImporter {
	return &CustomerImporter{
		path: filePath,
	}
}

// ImportDomainData reads and returns sorted customer domain data from CSV file.
func (ci CustomerImporter) ImportDomainData() ([]DomainData, error) {
	file, err := os.Open(*ci.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	data := make(map[string]uint64)

	// skip first line with headers
	line, readErr := csvReader.Read()
	if readErr != nil {
		fmt.Println(line, readErr)
		return nil, readErr
	}
	for line, readErr := csvReader.Read(); readErr != io.EOF; line, readErr = csvReader.Read() {
		if readErr != nil {
			return nil, readErr
		}
		email, domain, found := strings.Cut(line[2], "@")
		if email == "" || !found {
			return nil, fmt.Errorf("error invalid email address: %s", line[2])
		}
		data[domain] += 1
	}
	domainData := make([]DomainData, 0, len(data))
	for k, v := range data {
		domainData = append(domainData, DomainData{
			Domain:           k,
			CustomerQuantity: v,
		})
	}
	slices.SortFunc(domainData, func(l, r DomainData) int {
		return cmp.Compare(l.Domain, r.Domain)
	})
	return domainData, nil
}
