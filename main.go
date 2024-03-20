package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/RiverPhillips/raft/kv"
	"log/slog"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/RiverPhillips/raft/raft"
	_ "net/http/pprof"
)

var portFlag = flag.Int("port", 8080, "Port to listen on")
var memberIDFlag = flag.Int("id", 1, "ID of the member")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx, canc := context.WithCancel(ctx)
	defer canc()

	flag.Parse()

	clusterStr := os.Getenv("CLUSTER_MEMBERS")
	if clusterStr == "" {
		slog.Error("CLUSTER_MEMBERS environment variable must be set")
		os.Exit(1)
	}

	var members []*raft.ClusterMember

	memberStr := strings.Split(clusterStr, ",")
	for _, s := range memberStr {
		// Each s should be in the format id@addr
		formatRegex := `^\d+@.+$`
		if !regexp.MustCompile(formatRegex).MatchString(s) {
			slog.Error("member string is not in the correct format", "member", s)
			os.Exit(1)
		}

		ss := strings.Split(s, "@")
		id, err := strconv.Atoi(ss[0])
		if err != nil {
			slog.Error("failed to convert member id to int", "error", err, "member", s)
			os.Exit(1)
		}
		members = append(members, &raft.ClusterMember{
			Id:   raft.NewMemberId(uint16(id)),
			Addr: ss[1],
		})
	}

	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(h))

	sm := kv.NewKvStateMachine()

	raftServer := raft.NewServer(raft.NewMemberId(uint16(*memberIDFlag)), sm, members)

	if err := rpc.Register(raftServer); err != nil {
		slog.Error("failed to register rpc", "error", err)
		os.Exit(1)
	}
	rpc.HandleHTTP()

	srv := &http.Server{
		Handler: http.DefaultServeMux,
		Addr:    fmt.Sprintf(":%d", *portFlag),
	}

	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("failed to serve", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		// Wait for the server to start then dispatch a single command
		time.Sleep(3 * time.Second)

		if raftServer.State() == raft.Leader {
			slog.Info("Dispatching command to raft server")
			// Dispatch a command to the state machine
			setCmd := kv.SetCommand{Key: "test", Value: "test"}
			_, err := raftServer.ApplyCommand(raft.Command(setCmd.Serialize()))
			if err != nil && err.Error() != "not the leader" {
				panic(err)
			}

			time.Sleep(3 * time.Second)

			getCmd := kv.GetCommand{Key: "test"}
			result, err := raftServer.ApplyCommand(raft.Command(getCmd.Serialize()))
			if err != nil && err.Error() != "not the leader" {
				panic(err)
			}

			slog.Info("Result of get command", "result", string(result[0]))
			canc()
		}

	}()

	if err := raftServer.Start(ctx); err != nil {
		slog.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	slog.Info("Shutting down http server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Error shutting down server", "error", err)
	}
}
