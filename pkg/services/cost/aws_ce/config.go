package aws_ce

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

const (
	DefaultRegion = "us-east-1" // Default region if not specified in AWS profile
)

func LoadConfig(ctx context.Context, profile string) (*awssdk.Config, error) {
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile(profile),
		config.WithDefaultRegion(DefaultRegion),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	// Test the credentials
	_, err = awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("invalid AWS credentials for profile %s: %w", profile, err)
	}

	return &awsCfg, nil
}
