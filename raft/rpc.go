package raft

type RequestVoteArgs struct {
	Term int
	CandidateID string
	LastLogIndex int // index of candidate's last log entry (0 if empty)
	LastLogTerm int // term of candidate's last log entry (0 if empty)
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


type DebugStateArgs struct{}

type DebugStateReply struct{
	State string
	Term int
	LogLen int
	CommitIndex int
	LastApplied int
	WhoLeader string
}