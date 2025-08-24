package domain

type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
)

type AuditFinding struct {
	Id             string
	Resource       ResourceDef
	Issue          string // code of the issue, (e.g., auto_stop_disabled)
	Description    string // human-readable description
	Recommendation string
	Severity       Severity // low/medium/high
}

type AuditReport struct {
	Workspace    string
	ResourceType string
	Period       TimePeriod
	Summary      map[string]any // issue -> recommendation
	Findings     []AuditFinding
}
