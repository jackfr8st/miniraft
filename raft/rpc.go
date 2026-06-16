package raft

type RequestVoteArgs struct {
	Term int
	CandidateID string
}

type RequestVoteReply struct {
	Term int
	VoteGiven bool
}

