.PHONY: test test-race demo demo-block fmt vet

test:
	go test -count=1 ./...

test-race:
	CGO_ENABLED=1 go test -race -count=1 -v ./...

demo:
	go run ./cmd/edge-demo -policy drop-oldest -capacity 4

demo-block:
	go run ./cmd/edge-demo -policy block -capacity 2

fmt:
	gofmt -w .

vet:
	go vet ./...
