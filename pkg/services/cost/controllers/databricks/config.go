package databricks

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Host     string `mapstructure:"host" validate:"required"`
	Token    string `mapstructure:"token" validate:"required"`
	HTTPPath string `mapstructure:"http_path" validate:"required"`
	Catalog  string `mapstructure:"catalog"`
	Schema   string `mapstructure:"schema"`
}

func LoadConfig(profilePath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(profilePath)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse databricks config: %w", err)
	}
	return &cfg, nil
}
