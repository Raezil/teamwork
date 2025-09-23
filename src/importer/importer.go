package importer

import (
	"cmp"
	"encoding/csv"
	"helpers"
	"io"
	"os"
	"slices"
	"sync"
)

type DomainData struct {
	Domain           string
	CustomerQuantity uint64
}

type CustomerImporter struct {
	path *string
}

func NewCustomerImporter(filePath *string) *CustomerImporter {
	return &CustomerImporter{path: filePath}
}

func (ci CustomerImporter) ImportData(csvReader *csv.Reader) (map[string]uint64, error) {
	data := make(map[string]uint64)
	jobs := make(chan []string, 1000) // buffered channel for input lines
	results := make(chan string, 1000)

	var wg sync.WaitGroup
	numWorkers := 8 // tune based on CPU cores and workload
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go helpers.Worker(jobs, results, &wg)
	}

	// collector goroutine: aggregate domains
	var agg sync.WaitGroup
	agg.Add(1)
	go func() {
		defer agg.Done()
		for domain := range results {
			data[domain]++
		}
	}()

	// skip header
	_, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	// feed jobs
	for {
		line, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			close(jobs)
			close(results)
			return nil, readErr
		}
		jobs <- line
	}

	close(jobs)
	wg.Wait()
	close(results)
	agg.Wait()

	return data, nil
}

func (ci CustomerImporter) ImportDomainData() ([]DomainData, error) {
	file, err := os.Open(*ci.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	data, err := ci.ImportData(csvReader)
	if err != nil {
		return nil, err
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
