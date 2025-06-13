package databricks

import (
	"testing"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

// stubAnalyzer lets us simulate any Analyzer with preset outputs or errors.
type stubAnalyzer struct {
	rt         string
	raw        []domain.ResourceCost
	report     *domain.Report
	collectErr error
	reportErr  error
}

func (s *stubAnalyzer) GetResourceType() string {
	return s.rt
}
func (s *stubAnalyzer) CollectUsage(days int) ([]domain.ResourceCost, error) {
	return s.raw, s.collectErr
}
func (s *stubAnalyzer) GenerateReport(days int) (*domain.Report, error) {
	return s.report, s.reportErr
}

func TestNewDatabricksController_ValidAnalyzers_ShouldListSupportedResources(t *testing.T) {
	// Given
	a1 := &stubAnalyzer{rt: "foo"}
	a2 := &stubAnalyzer{rt: "bar"}

	// When
	ctrl, err := NewDatabricksController(a1, a2)

	// Then
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	supported := ctrl.GetSupportedResources()
	if len(supported) != 2 {
		t.Errorf("expected 2 resources, got %v", supported)
	}
}

func TestController_GetRawResourceCost_Success(t *testing.T) {
	// Given
	rc := domain.ResourceCost{Resource: domain.Resource{Name: "X"}}
	a := &stubAnalyzer{rt: "foo", raw: []domain.ResourceCost{rc}}
	ctrl, _ := NewDatabricksController(a)

	// When
	raw, err := ctrl.GetRawResourceCost("foo", 5)

	// Then
	if err != nil {
		t.Fatalf("GetRawResourceCost error: %v", err)
	}
	if len(raw) != 1 || raw[0].Resource.Name != "X" {
		t.Errorf("expected raw resource X, got %v", raw)
	}
}

func TestController_GetRawResourceCost_UnsupportedResource_ShouldError(t *testing.T) {
	// Given
	a := &stubAnalyzer{rt: "foo"}
	ctrl, _ := NewDatabricksController(a)

	// When
	_, err := ctrl.GetRawResourceCost("bar", 1)

	// Then
	expected := "unsupported resource type: bar"
	if err == nil || err.Error() != expected {
		t.Errorf("expected error %q, got %v", expected, err)
	}
}

func TestController_EstimateResourceCost_Success(t *testing.T) {
	// Given
	rep := &domain.Report{Title: "R"}
	a := &stubAnalyzer{rt: "foo", report: rep}
	ctrl, _ := NewDatabricksController(a)

	// When
	got, err := ctrl.EstimateResourceCost("foo", 5)

	// Then
	if err != nil {
		t.Fatalf("EstimateResourceCost error: %v", err)
	}
	if got.Title != "R" {
		t.Errorf("expected report title R, got %s", got.Title)
	}
}
