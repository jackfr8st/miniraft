package raft

import (
	"sync"
	"math/rand"
	"time"
	"log"
)

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

// Node Definition
type Node struct {
	sync.Mutex 

	id string
	peers map[string]string // peerID -> peerAddress
	state State 
	currentTerm int // current term number
	votedFor string 
	electionResetAt time.Time // last time the election timer was reset
	electionTimeout time.Duration // election timeout duration
	stopCh chan struct{} // channel to signal stopping the node
}

// a new Raft node with the given ID and peers
func NewNode(id string, peers map[string]string) *Node {
	n := &Node{
		id : id,
		peers : peers,
		state : Follower,
		stopCh : make(chan struct{}),
	}
	n.resetElectionTimer()
	return n
}

// resets the election timer to [150ms, 300ms]
func (n *Node) resetElectionTimer() {
	n.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.electionResetAt = time.Now()
}

func (n *Node) electionTimerLoop() {
	ticker := time.NewTicker(10*time.Millisecond)
	defer ticker.Stop()

	for {
		select {
			case <-n.stopCh:
				return
			case <-ticker.C:
				n.Lock()
				elapsed := time.Since(n.electionResetAt)
				timeOut := n.state != Leader && elapsed >= n.electionTimeout
				n.Unlock()

				if timeOut {
					n.startElection()
			}
		}
	}
}

func (n *Node) startElection() {
	n.Lock()
	n.state = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.resetElectionTimer()
	n.Unlock()

	log.Printf("Node %s starting election for term %d", n.id, term)
}

func( n *Node) Run() {
	go n.electionTimerLoop()
}

func (n *Node) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.Lock()
	defer n.Unlock()

	//candidate is behind, reject
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGiven = false
		return
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	if n.votedFor == "" || n.votedFor == args.CandidateID {
		n.votedFor = args.CandidateID
		n.resetElectionTimer()
		reply.VoteGiven = true
	}else {
		reply.VoteGiven = false
	}
	reply.Term = n.currentTerm

}