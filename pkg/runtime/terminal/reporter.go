package terminal

import (
	"fmt"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"io"
	"os"
	"text/template"
)

// Reporter outputs reports to the console in a formatted text form
type Reporter struct {
	writer io.Writer
}

// NewReporter creates a new console reporter
func NewReporter(writer io.Writer) *Reporter {
	if writer == nil {
		writer = os.Stdout
	}
	return &Reporter{writer: writer}
}

func (c *Reporter) Handle(report *domain.Report) error {
	tmpl := `
{{.Title}} ({{.Period.Duration}} days)
Period: {{.Period.Start.Format "2006-01-02"}} to {{.Period.End.Format "2006-01-02"}}
Total Amount: {{.Currency}} {{printf "%.2f" .TotalAmount}}

{{range .Sections}}
=== {{.Title}} ===
{{range $key, $value := .Summary}}
{{$key}}: {{$value}}
{{end}}
{{range .Details}}
- {{.Name}}: {{.Value}}{{if .Unit}} {{.Unit}}{{end}}
  {{.Description}}
{{end}}
{{end}}
`
	t, err := template.New("report").Parse(tmpl)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	return t.Execute(c.writer, report)
}
