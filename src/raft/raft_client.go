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
	cl.msgChan = make(chan AppendMessage, 400)
	cl.voteChan = make(chan RequestVoteArgs, 600)
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
				if (msg.Term != cl.raft.term && msg.MsgType == MsgHeartbeat) || cl.raft.leader != cl.raft.me {
					fmt.Printf("skip append msg from %d to %d which term(%d) is less than raft %d\n",
						msg.From, msg.To, msg.Term, cl.raft.term)
					break
				}
				msg.Id = idx
				idx += 1
				cl.raft.sendAppendEntries(&msg)
			}
			case msg := <- cl.voteChan : {
				if msg.MsgType == MsgStop || cl.stop {
					fmt.Printf("Stop client\n")
					return
				}
				if msg.Term < cl.raft.term || cl.raft.IsLeader() {
					fmt.Printf("skip vote msg from %d to %d which term(%d) is less than raft %d\n", msg.From, msg.To, msg.Term, cl.raft.term)
					break
				}
				cl.raft.sendRequestVote(&msg)
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
	if msg.MsgType == MsgAppend {
		cl.msgChan <- msg
	} else {
		select {
		case cl.msgChan <- msg : {
			fmt.Printf("bcast heartbeat failed. there is too much append message\n")
		}
		default:
			return
		}
	}


}

func (cl *RaftClient) VoteAsync(msg RequestVoteArgs) {
	select {
		case cl.voteChan <- msg : {
		}
	default:
		fmt.Printf("Vote failed. there is too much vote message\n")
		return
	}
}

