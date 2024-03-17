package raft

type ClusterMemberConfig struct {
	Id   uint16 `yaml:"id"`
	Addr string `yaml:"addr"`
}

type Config struct {
	Port           uint16                `yaml:"port"`
	ClusterMembers []ClusterMemberConfig `yaml:"cluster_members"`
	LogLevel       LogLevel              `yaml:"log_level"`
}

type LogLevel string

const (
	Debug LogLevel = "debug"
	Info  LogLevel = "info"
	Warn  LogLevel = "warn"
	Error LogLevel = "error"
)
