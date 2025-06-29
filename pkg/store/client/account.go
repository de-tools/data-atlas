package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"net/url"
)

type AccessToken struct {
	Token     string `json:"access_token"`
	Scope     string `json:"scope"`
	Type      string `json:"token_type"`
	ExpiresIn int64  `json:"expires_in"`
}

type Client struct {
	client *databricks.AccountClient
	config *config.Config
}

// NewAccountClient is not currently in use, but I left it here for reference.
// As we might want to provide cross-workspace functionality in the future.
func NewAccountClient(cfg *config.Config) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if !cfg.IsAccountClient() {
		return nil, fmt.Errorf("config must have an account client type")
	}

	client, err := databricks.NewAccountClient((*databricks.Config)(cfg))
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
		config: cfg,
	}, nil
}

func (ac *Client) ListWorkspaces(ctx context.Context) ([]provisioning.Workspace, error) {
	logger := zerolog.Ctx(ctx)

	workspaces, err := ac.client.Workspaces.List(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to list workspaces")
		return nil, err
	}

	return workspaces, nil
}

// GetWorkspaceToken - retrieves an access token for the specified workspace.
// With Account level config we might not have all the tokens for workspaces, and
// we might need to get a workspace token programmatically.
func (ac *Client) GetWorkspaceToken(ctx context.Context, workspace provisioning.Workspace) (*AccessToken, error) {
	logger := zerolog.Ctx(ctx)

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("scope", "all-apis")

	// TODO: pick url based on the type of the cloud provider in the workspace.
	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("https://%s.cloud.databricks.com/oidc/v1/token", workspace.DeploymentName),
		bytes.NewBufferString(data.Encode()),
	)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to create workspace token http request")
		return nil, err
	}

	req.SetBasicAuth(ac.config.ClientID, ac.config.ClientSecret)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get workspace token")
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			logger.Warn().Err(err).Msg("failed to close response body")
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to read workspace token response")
		return nil, err
	}

	var token AccessToken
	err = json.Unmarshal(body, &token)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal workspace token response: %w", err)
	}

	return &token, nil
}
