package adapters

import (
	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

func MapSeverityDomainToApi(s domain.Severity) api.Severity {
	switch s {
	case domain.SeverityLow:
		return api.SeverityLow
	case domain.SeverityMedium:
		return api.SeverityMedium
	case domain.SeverityHigh:
		return api.SeverityHigh
	default:
		return api.SeverityLow
	}
}

func MapTimePeriodDomainToApi(p domain.TimePeriod) api.TimePeriod {
	return api.TimePeriod{
		Start:    p.Start,
		End:      p.End,
		Duration: p.Duration,
	}
}

func MapAuditFindingDomainToApi(f domain.AuditFinding) api.AuditFinding {
	return api.AuditFinding{
		Id:             f.Id,
		Resource:       MapResourceDefinitionDomainToApi(f.Resource),
		Issue:          f.Issue,
		Description:    f.Description,
		Recommendation: f.Recommendation,
		Severity:       MapSeverityDomainToApi(f.Severity),
	}
}

func MapAuditReportDomainToApi(r domain.AuditReport) api.AuditReport {
	res := api.AuditReport{
		Workspace:    r.Workspace,
		ResourceType: r.ResourceType,
		Period:       MapTimePeriodDomainToApi(r.Period),
		Summary:      map[string]any{},
		Findings:     make([]api.AuditFinding, 0, len(r.Findings)),
	}
	// copy summary as-is
	for k, v := range r.Summary {
		res.Summary[k] = v
	}
	for _, f := range r.Findings {
		res.Findings = append(res.Findings, MapAuditFindingDomainToApi(f))
	}
	return res
}
