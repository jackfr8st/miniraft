package raft

import "testing"

func entry(term int, key, value string) LogEntry {
	return LogEntry{Term: term, Command: Command{Op: OpPut, Key: key, Value: value}}
}

// voteArgs builds the RequestVote a node would send when starting an election at term.
func voteArgs(n *Node, term int) *RequestVoteArgs {
	lastIndex, lastTerm := n.lastLogInfoLocked()
	return &RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
}

// A (leader, term 1) commits X at index 2 by replicating it to B. C never got X.
// A dies; C times out first and asks B for its vote at term 2. B must refuse,
// otherwise C could win and overwrite the committed X on B.
func TestStaleCandidateDeniedVote(t *testing.T) {
	base := entry(1, "a", "1")
	x := entry(1, "x", "committed")

	a := &Node{id: "nodeA", currentTerm: 1, log: []LogEntry{base, x}}
	b := &Node{id: "nodeB", currentTerm: 1, log: []LogEntry{base, x}}
	c := &Node{id: "nodeC", currentTerm: 1, log: []LogEntry{base}}

	var reply RequestVoteReply
	b.HandleRequestVote(voteArgs(c, 2), &reply)
	if reply.VoteGiven {
		t.Fatal("B voted for C, whose log is missing committed entry X")
	}
	if b.votedFor != "" {
		t.Errorf("B recorded votedFor=%q after refusing", b.votedFor)
	}
	if reply.Term != 2 || b.currentTerm != 2 {
		t.Errorf("B should still adopt the higher term: reply.Term=%d currentTerm=%d", reply.Term, b.currentTerm)
	}

	// A, which holds X, can still win B's vote in the same term.
	reply = RequestVoteReply{}
	b.HandleRequestVote(voteArgs(a, 2), &reply)
	if !reply.VoteGiven {
		t.Fatal("B refused A, whose log is as up to date as its own")
	}
}

func TestVoteUpToDateComparison(t *testing.T) {
	tests := []struct {
		name      string
		voterLog  []LogEntry
		candLog   []LogEntry
		wantGrant bool
	}{
		{"both empty", nil, nil, true},
		{"identical logs", []LogEntry{entry(1, "a", "1")}, []LogEntry{entry(1, "a", "1")}, true},
		{"same last term, candidate longer", []LogEntry{entry(1, "a", "1")}, []LogEntry{entry(1, "a", "1"), entry(1, "b", "2")}, true},
		{"same last term, candidate shorter", []LogEntry{entry(1, "a", "1"), entry(1, "b", "2")}, []LogEntry{entry(1, "a", "1")}, false},
		{"higher last term wins despite shorter log", []LogEntry{entry(1, "a", "1"), entry(1, "b", "2"), entry(1, "c", "3")}, []LogEntry{entry(2, "a", "1")}, true},
		{"lower last term loses despite longer log", []LogEntry{entry(2, "a", "1")}, []LogEntry{entry(1, "a", "1"), entry(1, "b", "2"), entry(1, "c", "3")}, false},
		{"empty candidate vs non-empty voter", []LogEntry{entry(1, "a", "1")}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voter := &Node{id: "voter", currentTerm: 2, log: tt.voterLog}
			cand := &Node{id: "cand", currentTerm: 2, log: tt.candLog}
			var reply RequestVoteReply
			voter.HandleRequestVote(voteArgs(cand, 3), &reply)
			if reply.VoteGiven != tt.wantGrant {
				t.Errorf("VoteGiven=%v, want %v", reply.VoteGiven, tt.wantGrant)
			}
		})
	}
}
