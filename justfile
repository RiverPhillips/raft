# Default recipe to run unit & cluster tests with race detector
test:
	go test -race ./...

# Run tests repeatedly
test-repeat count="20":
	go test -race -count={{count}} ./...

# Compile test binary and run with go stress tool (requires `go install golang.org/x/tools/cmd/stress@latest`)
stress:
	go test -c -race ./raft -o /tmp/raft.test
	stress /tmp/raft.test 
