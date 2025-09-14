# Product Overview

Data Atlas is a Databricks cost management and analysis tool that helps organizations monitor, analyze, and optimize their Databricks resource consumption and costs.

## Core Features

- **Cost Visualization**: Web-based dashboard for viewing resource costs across workspaces
- **Multi-Resource Support**: Track costs for clusters, warehouses, DLT pipelines, and model serving endpoints
- **Time-based Analysis**: Configurable time ranges for cost analysis (default 7-day intervals)
- **Workspace Management**: Multi-workspace support with resource-level granularity
- **Audit Capabilities**: Built-in auditing for DLT pipelines with configurable thresholds
- **Data Sync**: Background workflows to sync usage data from Databricks APIs

## Target Users

- Data platform teams managing Databricks environments
- FinOps teams tracking cloud data platform costs
- Engineering teams optimizing resource utilization

## Architecture

- **Backend**: Go-based REST API server with DuckDB for local data storage
- **Frontend**: React-based web dashboard with TypeScript and Tailwind CSS
- **Data Sources**: Databricks SDK for workspace/account APIs and SQL connector for usage data