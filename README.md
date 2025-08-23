# Usage

Build the project first.
```
> make build

> ./cost help
```

## Using API server
### Run locally
* Run server: `./cost -c  $HOME/.databrickscfg`
* Base URL: http://localhost:8080/api/v1
* Date format in queries: DD-MM-YYYY (e.g., 02-01-2006)

### APIs
* List workspaces - `curl -s http://localhost:8080/api/v1/workspaces | jq`
* List resources in a workspace -
`curl -s http://localhost:8080/api/v1/{workspace}/resources | jq`
* Resource cost for a single resource type - `curl
  http://localhost:8080/api/v1/workspaces/{workspace}/resources/{resource}/cost?from={from}&to={to} | jq`
* Resource cost for multiple resource types - `curl
  http://localhost:8080/api/v1/workspaces/{workspace}/resources/cost?resource={resource_1}&resource={resource_2}&from={from}&to={to} | jq`
* Start usage sync workflow - `curl -s -X POST http://localhost:8080/api/v1/workspaces/{workspace}/sync`
