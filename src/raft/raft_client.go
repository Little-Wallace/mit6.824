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
	prevIndex int
	next 	int
	matched int
	active	bool
	stop	bool
	term    *int
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
				if msg.Term != *cl.term {
					break
				}
				if msg.MsgType == MsgAppend {
					atomic.AddInt32(&cl.pending, -1)
					if cl.pending > 0 && cl.prevIndex == msg.PrevLogIndex{
						continue
					}
				}
				ok := cl.peer.Call("Raft.AppendEntries", &msg, &done)
				if ok {
					fmt.Printf("send append msg from %d to %d\n", msg.From, msg.To)
				}
			}
			}
		}
	}()
}

func (cl *RaftClient) Stop() {
	cl.stop = true
}

func (cl *RaftClient) Send(msg AppendMessage) {
	//if !force && atomic.LoadInt32(&cl.pending) > 100 {
	//	return
	//}
	if msg.MsgType == MsgAppend {
		atomic.AddInt32(&cl.pending, 1)
		cl.prevIndex = msg.PrevLogIndex
	}
	cl.msgChan <- msg
}

