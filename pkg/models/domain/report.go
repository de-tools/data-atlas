package domain

import "time"

// Report represents a complete analysis report
type Report struct {
	Title       string
	Period      TimePeriod
	Sections    []ReportSection
	TotalAmount float64
	Currency    string
}

// TimePeriod represents a time range for the report
type TimePeriod struct {
	Start    time.Time
	End      time.Time
	Duration int // in days
}

// ReportSection represents a logical section in the report
type ReportSection struct {
	Title    string
	Summary  map[string]interface{}
	Details  []ReportDetail
	Metadata map[string]interface{}
}

// ReportDetail represents detailed information within a section
type ReportDetail struct {
	Name        string
	Value       interface{}
	Unit        string
	Description string
}
