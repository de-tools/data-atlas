# Technology Stack

## Backend (Go)

- **Language**: Go 1.24.3
- **Web Framework**: Chi router for HTTP routing and middleware
- **Database**: DuckDB for local analytics storage
- **External APIs**: 
  - Databricks SDK Go for workspace/account management
  - Databricks SQL Go for usage data queries
- **Logging**: Zerolog for structured logging
- **CLI**: Cobra for command-line interface
- **Testing**: Standard Go testing with Testify assertions

## Frontend (React/TypeScript)

- **Language**: TypeScript 5.8.3
- **Framework**: React 19.1.0 with Vite build tool
- **Styling**: Tailwind CSS 4.1.11
- **Code Quality**: ESLint + Prettier with custom configurations
- **Testing**: Jest + React Testing Library

## Development Tools

- **Linting**: golangci-lint for Go, ESLint for TypeScript
- **Pre-commit**: Configured hooks for code quality
- **Build System**: Make for Go, npm scripts for frontend

## Common Commands

### Backend
```bash
# Development
make run                    # Run server locally
make build                  # Build binary
make test                   # Run unit tests
make itest                  # Run integration tests
make lint                   # Run linter
make fmt                    # Format code

# Server usage
./cost -c ~/.databrickscfg --sync    # Run with config and sync enabled
```

### Frontend
```bash
cd client
npm run dev                 # Development server
npm run build              # Production build
npm run lint               # Run linter
npm run preview            # Preview production build
```

## Configuration

- **Backend Config**: Uses `.databrickscfg` file for Databricks authentication
- **Environment**: `.env` file support for runtime configuration
- **Database**: Local DuckDB file (`data-atlas.db`)
- **Server**: Configurable host/port via `SERVER_HOST` and `SERVER_PORT` env vars