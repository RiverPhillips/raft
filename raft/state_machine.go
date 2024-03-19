package raft

type NoOpStateMachine struct{}

// Apply returns a result for every command
func (n *NoOpStateMachine) Apply(command ...Command) []Result {
	res := make([]Result, len(command))
	for i := range command {
		res[i] = Result{}
	}
	return res
}
