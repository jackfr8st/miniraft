package raft

import "sync"

type State int

const (
	Follower State = iota
	Candidate 
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Node struct {
	sync.Mutex 

	id string
	peers map[string]string
	state State
	currentTerm int
	votedFor string
}

func NewNode(id string, peers map[string]string) *Node {
	return &Node{
		id : id,
		peers : peers,
		state : Follower,
	}
}