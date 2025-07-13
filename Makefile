run:
	go run cmd/cli/main.go

build:
	go build -o cost cmd/web/main.go

test:
	go test ./... -v

itest:
	go test -v -tags=integration ./...

install-lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6

lint: install-lint
	golangci-lint run -c .golangci.yml ./...

tidy:
	go mod tidy

fmt: install-lint
	golangci-lint fmt
