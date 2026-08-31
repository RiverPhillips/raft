# Default recipe to run unit & cluster tests with race detector
test:
	go test -race ./...

# Compile test binary and run with go stress tool (requires `go install golang.org/x/tools/cmd/stress@latest`)
stress count="1000":
	go test -c -race ./raft -o /tmp/raft.test
	stress -failfast -count {{count}} /tmp/raft.test 

lint:
	golangci-lint run ./...

bench package="./...":
	go test -run=^$ -bench=. -benchmem {{package}}

# Record benchmark baseline (e.g. before changes)
bench-record target="old.txt" count="5" package="./...":
	go test -run=^$ -bench=. -benchmem -count={{count}} {{package}} | tee {{target}}

# Compare current benchmarks against a recorded baseline using benchstat
bench-compare baseline="old.txt" count="5" package="./...":
	@go test -run=^$ -bench=. -benchmem -count={{count}} {{package}} > /tmp/new_bench.txt
	benchstat {{baseline}} /tmp/new_bench.txt
