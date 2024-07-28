package raft

// NoOpStateMachine is a state machine that does nothing
// It's used for testing
type NoOpStateMachine struct{}

var _ StateMachine = &NoOpStateMachine{}

// Apply returns a result for every command
func (n *NoOpStateMachine) Apply(command ...Command) []Result {
	res := make([]Result, len(command))
	for i := range command {
		res[i] = Result{}
	}
	return res
}
