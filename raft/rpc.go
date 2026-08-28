package raft

type RequestVoteArgs struct {
	Term int
	CandidateID string
}

type RequestVoteReply struct {
	Term int
	VoteGiven bool
}

type AppendEntriesArgs struct {
	Term int 
	LeaderID string
}

type AppendEntriesReply struct {
	Term int
	Success bool
}