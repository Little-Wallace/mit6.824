package raft

import (
	"labrpc"
	"sync/atomic"
	"fmt"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	pending int32
}


func (cl *RaftClient) Start(replyChan chan<- AppendReply) {
	cl.msgChan = make(chan AppendMessage, 100)
	go func() {
		var done DoneReply
		for {
			select {
			case msg := <- cl.msgChan : {
				if msg.MsgType == MsgAppend {
					var reply AppendReply
					ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
					if ok {
						fmt.Printf("send append msg from %d to %d\n", msg.From, msg.To)
						replyChan <- reply
					}
				} else if msg.MsgType == MsgHeartbeat {
					cl.peer.Call("Raft.HeartBeat", &msg, &done)
				}
				atomic.AddInt32(&cl.pending, -1)
			}
			}
		}
	}()
}

func (cl *RaftClient) Send(msg AppendMessage) {
	if msg.MsgType == MsgHeartbeat && atomic.LoadInt32(&cl.pending) > 1 {
		return
	}
	atomic.AddInt32(&cl.pending, 1)
	cl.msgChan <- msg
}


