# Project Structure

## Root Level Organization

```
data-atlas/
├── cmd/                    # Application entry points
├── pkg/                    # Go packages (business logic)
├── client/                 # React frontend application
├── etc/                    # Configuration files and profiles
├── .kiro/                  # Kiro AI assistant configuration
└── [config files]         # Go mod, Makefile, etc.
```

## Backend Structure (`pkg/`)

### Core Packages
- **`models/`**: Data models organized by layer
  - `api/`: HTTP request/response models
  - `domain/`: Business domain models
  - `store/`: Database/storage models
- **`handlers/`**: HTTP request handlers (controllers)
- **`services/`**: Business logic and orchestration
- **`store/`**: Data access layer implementations
- **`server/`**: HTTP server setup and middleware
- **`adapters/`**: Model transformation utilities

### Service Layer Organization
- **`account/`**: Databricks account and workspace management
- **`workflow/`**: Background job orchestration
- **`config/`**: Configuration management

### Store Layer Organization
- **`databrickssdk/`**: Databricks API clients
- **`databrickssql/`**: SQL-based data access
- **`duckdb/`**: Local DuckDB storage implementations

## Frontend Structure (`client/src/`)

### Component Organization
- **`components/`**: Reusable UI components
  - `cost-view/`: Cost visualization components
  - `side-panel/`: Navigation and filter components
- **`services/`**: API clients and utilities
- **`types/`**: TypeScript type definitions
- **`hooks/`**: Custom React hooks
- **`mocks/`**: Mock data for development

## Naming Conventions

### Go Packages
- Use lowercase package names
- Group related functionality in packages
- Separate concerns by layer (handlers, services, store)
- Use descriptive names that reflect purpose

### React Components
- PascalCase for component files and names
- Group related components in folders
- Use descriptive names that reflect UI purpose
- Separate hooks, services, and types into dedicated folders

## Architecture Patterns

### Backend
- **Layered Architecture**: Clear separation between handlers, services, and store
- **Dependency Injection**: Services injected into handlers via constructors
- **Repository Pattern**: Store layer abstracts data access
- **Adapter Pattern**: Transform between model layers

### Frontend
- **Component Composition**: Small, focused components
- **Custom Hooks**: Extract stateful logic into reusable hooks
- **Service Layer**: Separate API calls from components
- **Type Safety**: Strong TypeScript typing throughout