package config

import (
	"context"
	"fmt"
	databricksconfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/rs/zerolog"
	"gopkg.in/ini.v1"
)

type Registry interface {
	GetProfiles(ctx context.Context) ([]domain.ConfigProfile, error)
	GetConfig(ctx context.Context, profile domain.ConfigProfile) (*databricksconfig.Config, error)
}

type CfgRegistry struct {
	cfg        *ini.File
	path       string
	profileMap map[domain.ConfigProfile]*databricksconfig.Config
}

func NewRegistry(path string) (*CfgRegistry, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, err
	}

	return &CfgRegistry{
		cfg:        cfg,
		path:       path,
		profileMap: make(map[domain.ConfigProfile]*databricksconfig.Config),
	}, nil
}

func (cr *CfgRegistry) Init(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)
	for _, section := range cr.cfg.Sections() {
		if len(section.Keys()) > 0 {
			profileName := section.Name()

			cfg, err := cr.loadConfig(ctx, profileName)
			if err != nil {
				return err
			}

			if cfg.IsAccountClient() {
				logger.Info().
					Msgf("profile %s is an account level profile, Skipped.", profileName)
				continue
			}

			profile := domain.ConfigProfile{Name: profileName, Type: domain.ProfileTypeWorkspace}
			if cfg, exists := cr.profileMap[profile]; !exists {
				cr.profileMap[profile] = cfg
			}
		}
	}
	return nil
}

func (cr *CfgRegistry) GetProfiles(_ context.Context) ([]domain.ConfigProfile, error) {
	profiles := make([]domain.ConfigProfile, 0, len(cr.profileMap))
	for profile := range cr.profileMap {
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func (cr *CfgRegistry) GetConfig(_ context.Context, profile domain.ConfigProfile) (*databricksconfig.Config, error) {
	if cfg, exists := cr.profileMap[profile]; exists {
		return cfg, nil
	}

	return nil, fmt.Errorf("profile %s not found in %s", profile, cr.path)
}

func (cr *CfgRegistry) loadConfig(_ context.Context, profile string) (*databricksconfig.Config, error) {
	profileValues := cr.cfg.Section(profile)
	if len(profileValues.Keys()) == 0 {
		return nil, fmt.Errorf("%s has no %s profile configured", cr.path, profile)
	}

	cfg := &databricksconfig.Config{}
	err := databricksconfig.ConfigAttributes.ResolveFromStringMapWithSource(
		cfg,
		profileValues.KeysHash(),
		databricksconfig.Source{Type: databricksconfig.SourceFile, Name: cr.path},
	)

	if err != nil {
		return nil, fmt.Errorf("%s %s profile: %w", cr.path, profile, err)
	}

	return cfg, nil
}
