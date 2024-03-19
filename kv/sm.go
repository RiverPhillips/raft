package kv

import "github.com/RiverPhillips/raft/raft"

type KvStateMachine struct {
}

var _ raft.StateMachine = (*KvStateMachine)(nil)

func (k KvStateMachine) Apply(commands ...raft.Command) []raft.Result {
	//TODO implement me
	panic("implement me")
}

func NewKvStateMachine() *KvStateMachine {
	return &KvStateMachine{}
}

func (*KvStateMachine) Get(key string) (string, error) {
	//TODO implement me
	// Call raft with a get command so we always get a consistent value
	panic("implement me")
}

func (*KvStateMachine) Set(key, value string) error {
	//TODO implement me
	panic("implement me")
}
