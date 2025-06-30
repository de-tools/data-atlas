package config

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go/config"
	"gopkg.in/ini.v1"
)

type Registry interface {
	GetProfiles(ctx context.Context) ([]string, error)
	GetConfig(ctx context.Context, profile string) (*config.Config, error)
}

type cfgRegistry struct {
	cfg *ini.File
}

func NewRegistry(path string) (Registry, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, err
	}
	return &cfgRegistry{cfg: cfg}, nil
}

func (cr *cfgRegistry) GetProfiles(_ context.Context) ([]string, error) {
	var profiles []string
	for _, section := range cr.cfg.Sections() {
		if len(section.Keys()) > 0 {
			profiles = append(profiles, section.Name())
		}
	}
	return profiles, nil
}

func (cr *cfgRegistry) GetConfig(_ context.Context, profile string) (*config.Config, error) {
	section := cr.cfg.Section(profile)
	if section == nil {
		return nil, fmt.Errorf("profile %s not found", profile)
	}

	host := section.Key("host").String()
	token := section.Key("token").String()

	return &config.Config{
		Host:  host,
		Token: token,
	}, nil
}
