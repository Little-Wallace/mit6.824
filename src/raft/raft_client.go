package raft

import (
	"labrpc"
	"time"
	"fmt"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	voteChan chan RequestVoteArgs
	next 	int
	matched int
	pending int32
	active	bool
	stop	bool
	raft  	*Raft
}


func (cl *RaftClient) Start() {
}

func (cl *RaftClient) Stop() {
}

func (cl *RaftClient) sendAppendEntries(msg AppendMessage) bool {
	start := time.Now()
	var reply AppendReply
	ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	if !ok && msg.MsgType == MsgAppend {
		ok = cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	}
	calcRuntime(start, "sendAppendEntries")
	if ok {
		fmt.Printf("send append msg success from %d to %d\n", msg.From, msg.To)
		cl.raft.msgChan <- reply
	}
	return ok
}

func (cl *RaftClient) sendRequestVote(args RequestVoteArgs) bool {
	start := time.Now()
	var reply RequestVoteReply
	if args.MsgType == MsgRequestPrevote {
		reply.MsgType = MsgRequestPrevoteReply
	} else {
		reply.MsgType = MsgRequestVoteReply
	}
	fmt.Printf("begin send request vote from %d to %d \n", args.From, args.To)
	ok := cl.peer.Call("Raft.RequestVote", &args, &reply)
	calcRuntime(start, "sendRequestVote")
	reply.To = args.To
	if ok {
		cl.raft.voteChan <- reply
	}
	return ok
}

func (cl *RaftClient) AppendAsync(msg AppendMessage) {
	go cl.sendAppendEntries(msg)
}

func (cl *RaftClient) VoteAsync(msg RequestVoteArgs) {
	go cl.sendRequestVote(msg)
}

