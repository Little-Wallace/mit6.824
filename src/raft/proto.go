package raft

type MessageType int
const (
	_ MessageType = iota
	MsgStop
	MsgPropose
	MsgHeartbeat
	MsgHeartbeatReply
	MsgAppendReply
	MsgAppend
	MsgRequestVote
)

type AppendMessage struct {
	MsgType			MessageType
	Term			int
	From			int
	To 				int
	Commited		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]Entry
}

type AppendReply struct {
	MsgType		MessageType
	Term        int
	From		int
	To 			int
	Commited	int
	Success		bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	From			int
	To 				int
	LastLogIndex 	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	To 			int
	VoteGranted	bool
}

type DoneReply struct {}

