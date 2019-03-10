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
	pending int
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
					cl.pending -= 1
					break
				}
				msg.Id = idx
				idx += 1
				cl.raft.sendAppendEntries(&msg)
				cl.pending -= 1
			}
			case msg := <- cl.voteChan : {
				if msg.MsgType == MsgStop || cl.stop {
					fmt.Printf("Stop client\n")
					return
				}
				if !cl.raft.IsCandidate() {
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
		cl.pending += 1
		cl.msgChan <- msg
	} else if cl.pending < 3 {
		select {
		case cl.msgChan <- msg : {
			cl.pending += 1
		}
		default:
			fmt.Printf("bcast heartbeat failed. there is too much append message\n")
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

