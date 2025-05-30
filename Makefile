run:
	go run cmd/cli/main.go

build:
	go build -o cost cmd/cli/main.go

test:
	go test ./... -v

lint:
	golangci-lint run ./...

tidy:
	go mod tidy
