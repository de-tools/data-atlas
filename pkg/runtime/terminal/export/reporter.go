package export

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type TableConfig struct {
	NameWidth        int
	ValueWidth       int
	UnitWidth        int
	DescriptionWidth int
}

func DefaultTableConfig() TableConfig {
	return TableConfig{
		NameWidth:        40,
		ValueWidth:       40,
		UnitWidth:        12,
		DescriptionWidth: 54,
	}
}

type Reporter struct {
	writer io.Writer
	config TableConfig
}

func NewReporter(writer io.Writer) *Reporter {
	if writer == nil {
		writer = os.Stdout
	}
	return &Reporter{
		writer: writer,
		config: DefaultTableConfig(),
	}
}

func (c *Reporter) Handle(report *domain.Report) error {
	funcMap := template.FuncMap{
		"formatRow": func(name string, value interface{}, unit string, desc string) string {
			unitStr := unit
			if unit == "" {
				unitStr = strings.Repeat(" ", c.config.UnitWidth)
			}
			return fmt.Sprintf("| %-*s | %-*v | %-*s | %-*s |",
				c.config.NameWidth, name,
				c.config.ValueWidth, value,
				c.config.UnitWidth, unitStr,
				c.config.DescriptionWidth, desc)
		},
		"separator": func() string {
			return fmt.Sprintf("+%s+%s+%s+%s+",
				strings.Repeat("-", c.config.NameWidth+2),
				strings.Repeat("-", c.config.ValueWidth+2),
				strings.Repeat("-", c.config.UnitWidth+2),
				strings.Repeat("-", c.config.DescriptionWidth+2))
		},
	}

	tmpl := `
{{.Title}} ({{.Period.Duration}} days)

Active Period: {{.Period.Start.Format "2006-01-02"}} to {{.Period.End.Format "2006-01-02"}}
Total Amount: {{.Currency}} {{printf "%.2f" .TotalAmount}}

{{range .Sections}}
=== {{.Title}} ===
{{range $key, $value := .Summary}}
{{$key}}: {{$value}}
{{end}}

{{separator}}
{{formatRow "Name" "Value" "Unit" "Description"}}
{{separator}}
{{range .Details}}{{if and (ne .Name "Active Period Start") (ne .Name "Active Period End")}}{{formatRow .Name .Value .Unit .Description}}
{{end}}{{end}}{{separator}}
{{end}}
`

	t, err := template.New("report").Funcs(funcMap).Parse(tmpl)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	return t.Execute(c.writer, report)
}
