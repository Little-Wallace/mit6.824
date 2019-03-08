package raft

import (
	"labrpc"
	"fmt"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	next 	int
	matched int
	active	bool
	stop	bool
	raft  	*Raft
}


func (cl *RaftClient) Start() {
	cl.msgChan = make(chan AppendMessage, 100)
	cl.stop = false
	go func() {
		idx := 0
		for !cl.stop{
			select {
			case msg := <- cl.msgChan : {
				if msg.MsgType == MsgStop || cl.stop {
					fmt.Printf("Stop client\n")
					return
				}
				if msg.Term != cl.raft.term {
					fmt.Printf("skip append msg from %d to %d which term(%d) is less than raft %d\n", msg.From, msg.To, msg.Term, cl.raft.term)
					break
				}
				msg.Id = idx
				idx += 1
				var reply AppendReply
				ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
				if ok {
					fmt.Printf("send append msg from %d to %d\n", msg.From, msg.To)
					cl.raft.handleAppendReply(&reply)
				}
			}
			}
		}
		fmt.Printf("Stop client\n")
	}()
}

func (cl *RaftClient) Stop() {
	var msg AppendMessage
	msg.MsgType = MsgStop
	cl.msgChan <- msg
	cl.stop = true
}

func (cl *RaftClient) Send(msg AppendMessage) {
	//if !force && atomic.LoadInt32(&cl.pending) > 100 {
	//	return
	//}
	cl.msgChan <- msg
}

