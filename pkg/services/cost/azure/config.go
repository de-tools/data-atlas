package azure

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"gopkg.in/ini.v1"
)

const (
	DefaultProfile = "default"
	DefaultRegion  = "eastus"
)

type Config struct {
	SubscriptionID string
	TenantID       string
	ClientID       string
	Region         string
	Credentials    *azidentity.AzureCLICredential
}

func LoadConfig(ctx context.Context, profile string) (*Config, error) {
	if profile == "" {
		profile = DefaultProfile
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("unable to get home directory: %w", err)
	}

	configPath := filepath.Join(homeDir, ".azure", "config")
	cfg, err := ini.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to load Azure config file: %w", err)
	}

	section, err := cfg.GetSection(profile)
	if err != nil {
		return nil, fmt.Errorf("profile %s not found in Azure config: %w", profile, err)
	}

	config := &Config{
		SubscriptionID: section.Key("subscription").String(),
		TenantID:       section.Key("tenant").String(),
		ClientID:       section.Key("client_id").String(),
		Region:         section.Key("region").MustString(DefaultRegion),
	}

	if config.SubscriptionID == "" {
		return nil, fmt.Errorf("subscription ID not found in profile %s", profile)
	}

	credentials, err := getCredentials(profile)
	if err != nil {
		return nil, fmt.Errorf("failed to get Azure credentials: %w", err)
	}
	config.Credentials = credentials
	return config, nil
}

func getCredentials(profile string) (*azidentity.AzureCLICredential, error) {
	// Set AZURE_PROFILE environment variable to make Azure SDK use the right profile
	if err := os.Setenv("AZURE_PROFILE", profile); err != nil {
		return nil, fmt.Errorf("failed to set Azure profile: %w", err)
	}

	// AzureCLICredential will use the profile specified above
	cred, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure CLI credential: %w", err)
	}

	return cred, nil
}
