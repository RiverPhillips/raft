# Default recipe to run unit & cluster tests with race detector
test:
	go test -race ./...

# Compile test binary and run with go stress tool (requires `go install golang.org/x/tools/cmd/stress@latest`)
stress count="1000":
	go test -c -race ./raft -o /tmp/raft.test
	stress -failfast -count {{count}} /tmp/raft.test 
