package internal

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/jedib0t/go-pretty/table"
)

type WalVerifyOutputType int

const (
	WalVerifyTableOutput WalVerifyOutputType = iota + 1
	WalVerifyJsonOutput
)

// WalVerifyOutputWriter writes the output of wal-verify command execution result
type WalVerifyOutputWriter interface {
	Write(result WalVerifyResult) error
}

// WalVerifyJsonOutputWriter writes the detailed JSON output
type WalVerifyJsonOutputWriter struct {
	output io.Writer
}

func (writer *WalVerifyJsonOutputWriter) Write(result WalVerifyResult) error {
	bytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = writer.output.Write(bytes)
	return err
}

// WalVerifyTableOutputWriter writes the output as pretty table
type WalVerifyTableOutputWriter struct {
	output io.Writer
}

func (writer *WalVerifyTableOutputWriter) Write(result WalVerifyResult) error {
	fmt.Printf("[WAL segments verification] Status: %s\n", result.WalIntegrityCheckResult.Status)
	fmt.Printf("[WAL segments verification] Details:\n")
	// write detailed info about WAL integrity scan
	writer.writeTable(result.WalIntegrityCheckResult.SegmentSequences)

	// write timeline verification result
	fmt.Printf("[Timeline verification] Status: %s\n",
		result.TimelineVerifyResult.Status)
	fmt.Printf("[Timeline verification] Highest timeline found in storage: %d\n",
		result.TimelineVerifyResult.HighestStorageTimelineId)
	fmt.Printf("[Timeline verification] Current cluster timeline: %d\n",
		result.TimelineVerifyResult.CurrentTimelineId)

	return nil
}

func (writer *WalVerifyTableOutputWriter) writeTable(scanResult []*WalIntegrityScanSegmentSequence) {
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(writer.output)
	defer tableWriter.Render()
	tableWriter.AppendHeader(table.Row{"TLI", "Start", "End", "Segments count", "Status"})

	for _, row := range scanResult {
		tableWriter.AppendRow(table.Row{row.TimelineId, row.StartSegment, row.EndSegment, row.SegmentsCount, row.Status})
	}
}

func NewWalVerifyOutputWriter(outputType WalVerifyOutputType, output io.Writer) WalVerifyOutputWriter {
	switch outputType {
	case WalVerifyTableOutput:
		return &WalVerifyTableOutputWriter{output: output}
	case WalVerifyJsonOutput:
		return &WalVerifyJsonOutputWriter{output: output}
	default:
		return &WalVerifyJsonOutputWriter{output: output}
	}
}
