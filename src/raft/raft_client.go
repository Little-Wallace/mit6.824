package raft

import (
	"labrpc"
	"time"
	"fmt"
	"sync/atomic"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	voteChan chan RequestVoteArgs
	next 	int
	matched int
	lastAppendTime time.Time
	active	bool
	stop	int32
	raft  	*Raft
}

func (cl *RaftClient) PassAppendTimeout() bool {
	t := time.Now()
	if t.Sub(cl.lastAppendTime).Seconds() < 1.0 {
		return true
	}
	return false
}

func (cl *RaftClient) Start() {
	cl.stop = 0
}

func (cl *RaftClient) Stop() {
	atomic.StoreInt32(&cl.stop, 1)
}

func (cl *RaftClient) sendAppendEntries(msg AppendMessage) bool {
	start := time.Now()
	var reply AppendReply
	ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	ed := time.Now()
	if !ok && ed.Sub(start).Seconds() < 0.2 {
		ok = cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	}
	//calcRuntime(start, "sendAppendEntries")
	if ok && atomic.LoadInt32(&cl.stop) == 0 {
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
	if ok && atomic.LoadInt32(&cl.stop) == 0 {
		cl.raft.voteChan <- reply
	}
	return ok
}

func (cl *RaftClient) AppendAsync(msg AppendMessage) {
	if atomic.LoadInt32(&cl.stop) != 0 {
		return
	}
	go cl.sendAppendEntries(msg)
}

func (cl *RaftClient) VoteAsync(msg RequestVoteArgs) {
	if atomic.LoadInt32(&cl.stop) != 0 {
		return
	}
	go cl.sendRequestVote(msg)
}

