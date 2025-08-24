package domain

type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

type AuditFinding struct {
	ID             string
	Title          string
	Severity       Severity // low/medium/high
	Resource       ResourceDef
	Issue          string
	Recommendation string
}

type AuditReport struct {
	Workspace    string
	ResourceType string
	Period       TimePeriod
	Summary      map[string]any // issue -> recommendation
	Findings     []AuditFinding
}
