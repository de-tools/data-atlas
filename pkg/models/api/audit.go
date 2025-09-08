package api

import "time"

type Severity string

const (
	SeverityLow    Severity = "low"
	SeverityMedium Severity = "medium"
	SeverityHigh   Severity = "high"
)

type AuditFinding struct {
	Id             string      `json:"id"`
	Resource       ResourceDef `json:"resource"`
	Issue          string      `json:"issue"`
	Description    string      `json:"description"`
	Recommendation string      `json:"recommendation"`
	Severity       Severity    `json:"severity"`
}

type TimePeriod struct {
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Duration int       `json:"duration_days"`
}

type AuditReport struct {
	Workspace    string                 `json:"workspace"`
	ResourceType string                 `json:"resource_type"`
	Period       TimePeriod             `json:"period"`
	Summary      map[string]interface{} `json:"summary"`
	Findings     []AuditFinding         `json:"findings"`
}
