run:
	go run cmd/web/main.go

build:
	go build -o cost cmd/web/main.go

test:
	go test ./... -v

itest:
	go test -v -tags=integration ./...

lint:
	golangci-lint run -c .golangci.yml ./...

tidy:
	go mod tidy

fmt:
	golangci-lint fmt
