package exporter

import (
	"encoding/csv"
	"fmt"
	"importer/customerimporter"
	"io"
	"os"
	"strconv"
)

type CustomerExporter struct {
	outputPath *string
}

// NewCustomerExporter returns a new CustomerExporter that writes customer domain data to specified file.
func NewCustomerExporter(outputPath *string) *CustomerExporter {
	return &CustomerExporter{
		outputPath: outputPath,
	}
}

// ExportData writes sorted customer domain data to a CSV file. If file already exists, it will
// be truncated.
func (ex CustomerExporter) ExportData(data []customerimporter.DomainData) error {
	if data == nil {
		return fmt.Errorf("error provided data is empty (nil)")
	}
	outputFile, err := os.Create(*ex.outputPath)
	if err != nil {
		return fmt.Errorf("error creating new file for saving: %v", err)
	}
	defer outputFile.Close()
	return exportCsv(data, outputFile)
}

func exportCsv(data []customerimporter.DomainData, output io.Writer) error {
	headers := []string{"domain", "number_of_customers"}
	csvWriter := csv.NewWriter(output)
	defer func() error {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return err
		}
		return nil
	}()
	if err := csvWriter.Write(headers); err != nil {
		return err
	}
	for _, v := range data {
		pair := []string{v.Domain, strconv.FormatUint(v.CustomerQuantity, 10)}
		if err := csvWriter.Write(pair); err != nil {
			return err
		}
	}
	return nil
}
