package registry

import (
	"github.com/databricks/databricks-sdk-go/config"
)

type ConfigRegistry interface {
	GetProfiles() ([]string, error)
	GetConfig(profile string) (*config.Config, error)
}
