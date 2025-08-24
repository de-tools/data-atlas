package domain

type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
)

type AuditFinding struct {
	Resource       ResourceDef
	Issue          string
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
