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
}


func (cl *RaftClient) Start() {
	cl.msgChan = make(chan AppendMessage, 100)
	go func() {
		var done DoneReply
		for {
			select {
			case msg := <- cl.msgChan : {
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

func (cl *RaftClient) Send(msg AppendMessage, force bool) {
	if !force && atomic.LoadInt32(&cl.pending) > 100 {
		return
	}
	atomic.AddInt32(&cl.pending, 1)
	cl.msgChan <- msg
}

