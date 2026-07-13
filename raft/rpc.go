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
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type Op int

const (
	OpPut Op = iota
	OpDelete
)

type Command struct {
	Op Op
	Key string
	Value string
}

type LogEntry struct {
	Term int
	Command Command
}

type ClientPutArgs struct {
	Key string
	Value string
}

type ClientPutReply struct {
	Success bool
	WhoLeader string  // who we think is the leader
}

type ClientGetArgs struct {
	Key string
}

type ClientGetReply struct {
	Value string
	Found bool
	Success bool
	WhoLeader string
}
