package snowflake

import (
	"fmt"
	"github.com/snowflakedb/gosnowflake"
	"github.com/spf13/viper"
)

type Config struct {
	Account   string `mapstructure:"account" validate:"required"`
	User      string `mapstructure:"user" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Database  string `mapstructure:"database"`
	Warehouse string `mapstructure:"warehouse"`
	Role      string `mapstructure:"role"`
}

// LoadConfig loads configuration from the specified profile path
func LoadConfig(profilePath string) (*gosnowflake.Config, error) {
	v := viper.New()
	v.SetConfigFile(profilePath)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config gosnowflake.Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to parse snowflake config: %w", err)
	}
	return &config, nil
}
