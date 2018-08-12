package raft

import (
	"labrpc"
	"fmt"
	"sync/atomic"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	pending int32
	next 	int
	matched int
	active	bool
	stop	bool
}


func (cl *RaftClient) Start() {
	cl.msgChan = make(chan AppendMessage, 100)
	cl.stop = false
	go func() {
		var done DoneReply
		for !cl.stop{
			select {
			case msg := <- cl.msgChan : {
				if msg.MsgType == MsgStop {
					return
				}
				ok := cl.peer.Call("Raft.AppendEntries", &msg, &done)
				if ok {
					fmt.Printf("send append msg from %d to %d\n", msg.From, msg.To)
				}
				atomic.AddInt32(&cl.pending, -1)
			}
			}
		}
	}()
}

func (cl *RaftClient) Stop() {
	cl.stop = true
}

func (cl *RaftClient) Send(msg AppendMessage, force bool) {
	if !force && atomic.LoadInt32(&cl.pending) > 100 {
		return
	}
	atomic.AddInt32(&cl.pending, 1)
	cl.msgChan <- msg
}

