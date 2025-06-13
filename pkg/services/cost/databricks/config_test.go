package databricks

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_ValidYAML_PopulatesAllFields(t *testing.T) {
	// Given
	dir := t.TempDir()
	path := filepath.Join(dir, "valid.yaml")
	// No indentation inside the backtick block to avoid YAML parsing errors
	content := `host: "example.com:443"
token: "tok"
http_path: "/sql/1.0/warehouses/wh"
catalog: "main"
schema: "default"`
	err := os.WriteFile(path, []byte(content), 0o644)
	if err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	// When
	cfg, err := LoadConfig(path)

	// Then
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Host != "example.com:443" {
		t.Errorf("expected Host=example.com:443, got %s", cfg.Host)
	}
	if cfg.Token != "tok" {
		t.Errorf("expected Token=tok, got %s", cfg.Token)
	}
	if cfg.HTTPPath != "/sql/1.0/warehouses/wh" {
		t.Errorf("expected HTTPPath=/sql/1.0/warehouses/wh, got %s", cfg.HTTPPath)
	}
	if cfg.Catalog != "main" {
		t.Errorf("expected Catalog=main, got %s", cfg.Catalog)
	}
	if cfg.Schema != "default" {
		t.Errorf("expected Schema=default, got %s", cfg.Schema)
	}
}

func TestLoadConfig_InvalidYAML_ReturnsError(t *testing.T) {
	// Given
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	err := os.WriteFile(path, []byte("host: example:443: bad"), 0o644)
	if err != nil {
		t.Fatalf("failed to write bad config: %v", err)
	}

	// When
	_, err = LoadConfig(path)

	// Then
	if err == nil {
		t.Error("expected error for invalid YAML, got nil")
	}
}
