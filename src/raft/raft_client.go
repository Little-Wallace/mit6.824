package raft

import (
	"labrpc"
	"fmt"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	voteChan chan RequestVoteArgs
	next 	int
	matched int
	active	bool
	stop	bool
	raft  	*Raft
}


func (cl *RaftClient) Start() {
	cl.msgChan = make(chan AppendMessage, 100)
	cl.voteChan = make(chan RequestVoteArgs, 100)
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
				if msg.Term != cl.raft.term || cl.raft.leader != cl.raft.me {
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
			case msg := <- cl.voteChan : {
				if msg.MsgType == MsgStop || cl.stop {
					fmt.Printf("Stop client\n")
					return
				}
				if msg.Term < cl.raft.term {
					fmt.Printf("skip vote msg from %d to %d which term(%d) is less than raft %d\n", msg.From, msg.To, msg.Term, cl.raft.term)
					break
				}
				cl.raft.sendRequestVote(msg)
			}
			}
		}
		fmt.Printf("Stop client\n")
	}()
}

func (cl *RaftClient) Stop() {
	var msg AppendMessage
	msg.MsgType = MsgStop
	var v RequestVoteArgs
	v.MsgType = MsgStop
	cl.msgChan <- msg
	cl.voteChan <- v
	cl.stop = true
}

func (cl *RaftClient) AppendAsync(msg AppendMessage) {
	cl.msgChan <- msg
}

func (cl *RaftClient) VoteAsync(msg RequestVoteArgs) {
	select {
		case cl.voteChan <- msg : {
		}
	default:
		fmt.Printf("Vote failed. there is too much vote message")
		return
	}
}

