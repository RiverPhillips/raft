package kv

import (
	"github.com/RiverPhillips/raft/raft"
	"sync"
)

type KvStateMachine struct {
	db sync.Map
}

// EncodedCommand is the format of the command that is sent to the state machine
// First byte is the command type
// 0 for get, 1 for set
// Next 4 bytes are the length of the key
// Next n bytes are the key
// Next 4 bytes are the length of the value
// Next n bytes are the value
type EncodedCommand []byte

type StateMachineCommand interface {
	Serialize() EncodedCommand
}

type GetCommand struct {
	Key string
}

type SetCommand struct {
	Key   string
	Value string
}

func (g *GetCommand) Serialize() EncodedCommand {
	res := make([]byte, 1)
	res[0] = 0
	keyLen := uint32(len(g.Key))
	res = append(res, byte(keyLen>>24), byte(keyLen>>16), byte(keyLen>>8), byte(keyLen))
	res = append(res, []byte(g.Key)...)
	return res
}

func (s *SetCommand) Serialize() EncodedCommand {
	res := make([]byte, 1)
	res[0] = 1
	keyLen := uint32(len(s.Key))
	res = append(res, byte(keyLen>>24), byte(keyLen>>16), byte(keyLen>>8), byte(keyLen))
	res = append(res, []byte(s.Key)...)
	valueLen := uint32(len(s.Value))
	res = append(res, byte(valueLen>>24), byte(valueLen>>16), byte(valueLen>>8), byte(valueLen))
	res = append(res, []byte(s.Value)...)
	return res
}

func DeserializeCommand(data EncodedCommand) StateMachineCommand {
	switch data[0] {
	case 0:
		keyLen := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])
		key := string(data[5 : 5+keyLen])
		return &GetCommand{Key: key}
	case 1:
		keyLen := uint32(data[1])<<24 | uint32(data[2])<<16 | uint32(data[3])<<8 | uint32(data[4])
		key := string(data[5 : 5+keyLen])
		valueLen := uint32(data[5+keyLen])<<24 | uint32(data[5+keyLen+1])<<16 | uint32(data[5+keyLen+2])<<8 | uint32(data[5+keyLen+3])
		value := string(data[5+keyLen+4 : 5+keyLen+4+valueLen])
		return &SetCommand{Key: key, Value: value}
	}
	return nil
}

var _ raft.StateMachine = (*KvStateMachine)(nil)

func (k *KvStateMachine) Apply(commands ...raft.Command) []raft.Result {
	res := make([]raft.Result, len(commands))
	for i, cmd := range commands {
		smCmd := DeserializeCommand(EncodedCommand(cmd))
		switch c := smCmd.(type) {
		case *GetCommand:
			val, ok := k.db.Load(c.Key)
			if !ok {
				res[i] = raft.Result{}
			}
			res[i] = raft.Result(val.(string))
		case *SetCommand:
			k.db.Store(c.Key, c.Value)
			res[i] = raft.Result{}
		}
	}
	return res
}

func NewKvStateMachine() *KvStateMachine {
	return &KvStateMachine{}
}
