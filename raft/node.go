package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
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
	log []LogEntry // log entries
	commitIndex int // hightest idx that is committed  
	lastApplied int // highest idx that this node has applied to its state
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
	n.votedFor = n.id
	n.resetElectionTimer()
	log.Printf("[%s] starting election for term %d", n.id, term)
	n.Unlock()

	votes := 1
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for peerID, addr := range n.peers{
		wg.Add(1)

		go func(peerID, addr string){
			defer wg.Done()

			args  := &RequestVoteArgs{Term: term, CandidateID: n.id}
			var reply RequestVoteReply
			if err := callRPC(addr, "RaftRPC.RequestVote", args, &reply); err != nil{
				return
				// log.Printf("[%s] failed to request vote from %s: %v", n.id, peerID, err)
			}

			voteMu.Lock()
			defer voteMu.Unlock()
			if reply.VoteGiven {
				votes ++
			} else if reply.Term > term {
				n.Lock()
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.state = Follower
					n.votedFor = ""
				}
				n.Unlock()
			}
		}(peerID, addr)
	}

	wg.Wait()
	majority := len(n.peers)/2 + 1
	n.Lock()
	defer n.Unlock()
	if n.state == Candidate && n.currentTerm == term && votes >= majority{
		n.state = Leader
		log.Printf("[%s] won election for term %d with %d votes", n.id, term, votes)
		go n.heartbeatLoop(term)
	}
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

func (n *Node) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.Lock()
	defer n.Unlock()

	//leader is behind, reject
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return
	}


	if args.Term > n.currentTerm || n.state == Candidate{
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	n.state = Follower
	n.resetElectionTimer()

	//consistency check on followers
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(n.log) {
			// we dont have an entry => cant verify => reject
			reply.Term = n.currentTerm
			reply.Success = false
			return
		}

		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			//have entry but different term => histories diverged => reject
			reply.Term = n.currentTerm
			reply.Success = false
			return
		}
	}

	reply.Term = n.currentTerm
	reply.Success = true
}

func (n *Node) heartbeatLoop(term int){
	ticker := time.NewTicker(50*time.Millisecond)
	defer ticker.Stop()

	for {
		select {
			case <-n.stopCh:
				return
			case <-ticker.C:
				n.Lock()
				stillLeader := n.state == Leader && n.currentTerm == term
				n.Unlock()

				if !stillLeader {
					return 
				}
				for _, addr := range n.peers {
					go func(addr string){
						args := &AppendEntriesArgs{Term: term, LeaderID: n.id}
						var reply AppendEntriesReply
						if err := callRPC(addr, "RaftRPC.AppendEntries", args, &reply); err != nil {
							log.Printf("[%s] failed to send heartbeat to %s: %v", n.id, addr, err)
						}
					}(addr)
				}
		}
	}
}

func( n *Node) Run() {
	go n.electionTimerLoop()
	go n.statusLoop()
}
		
func (n *Node) statusLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.Lock()
			log.Printf("[%s] STATUS state=%s term=%d", n.id, n.state, n.currentTerm)
			n.Unlock()
		}
	}
}