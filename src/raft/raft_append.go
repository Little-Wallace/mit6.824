package raft

import "fmt"

func (rf *Raft) AppendEntries(args *AppendMessage, reply *AppendReply) {
	rf.handleAppendEntries(args, reply)
}

func (rf *Raft) HeartBeat(args *AppendMessage, done* DoneReply) {
	rf.msgChan <- *args
}

func (rf *Raft) AppendEntriesResponse(args *AppendReply, done* DoneReply) {
	rf.replyChan <- *args
}

func (rf *Raft) handleHeartbeat(msg *AppendMessage) AppendReply{
	reply := AppendReply{
		To: msg.From,
		From: rf.me,
		MsgType: getResponseType(msg.MsgType),
	}
	if !rf.checkTerm(msg.From, msg.Term, msg.MsgType) {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = 0
		return reply
	}
	fmt.Printf("%d(index: %d) access heartbeat from %d %d\n", rf.me, rf.raftLog.commited, msg.From, msg.Commited)
	reply.Success = true
	reply.Term = MaxInt(rf.term, reply.Term)
	rf.lastElection = 0
	rf.term = msg.Term
	if msg.Commited > rf.raftLog.commited {
		rf.raftLog.commited = msg.Commited
		rf.maybeApply()
	}
	return reply
}

func (rf *Raft) handleAppendEntries(args *AppendMessage, reply *AppendReply)  {
	if !rf.checkTerm(args.From, args.Term, args.MsgType) {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = 0
		return
	}
	index := len(rf.raftLog.Entries)
	if args.PrevLogIndex < rf.raftLog.commited {
		fmt.Printf("%d(index: %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, args.From, args.PrevLogIndex)
		reply.Success = true
		reply.Commited = rf.raftLog.commited
		reply.Term = rf.term
		return
	} else if args.PrevLogIndex >= index {
		fmt.Printf("%d(index: %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, args.From, args.PrevLogIndex)
		reply.Success = false
		reply.Commited = rf.raftLog.commited
		reply.Term = rf.term
		return
	}
	if rf.raftLog.Entries[args.PrevLogIndex].Term == args.PrevLogTerm {
		lastIndex := args.PrevLogIndex
		for idx, e := range args.Entries {
			e.Index = idx + args.PrevLogIndex + 1
			e.Term = args.Term
			if e.Index >= index {
				rf.raftLog.Entries = append(rf.raftLog.Entries, e)
			} else {
				rf.raftLog.Entries[e.Index] = e
			}
			lastIndex = e.Index
			m := rf.createApplyMsg(e)
			if m.CommandValid {
				rf.raftLog.pk = m.CommandIndex
			}
		}
		fmt.Printf("%d commit to %d -> min(%d, %d) all msg: %d -> %d, preindex :%d\n", rf.me, rf.raftLog.commited,
			args.Commited, lastIndex, index, len(rf.raftLog.Entries), args.PrevLogIndex)
		rf.raftLog.commited = MinInt(args.Commited, lastIndex)
		reply.Term = rf.term
		reply.Commited = lastIndex
		reply.Success = true
		rf.maybeApply()
	} else {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = index - 1
		e := rf.raftLog.Entries[args.PrevLogIndex]
		fmt.Printf("%d(index: %d, term: %d) reject append entries from %d(prev index: %d, term: %d)\n",
			rf.me, e.Index, e.Term, args.From, args.PrevLogIndex, args.PrevLogTerm)
	}
}

func getResponseType(msg MessageType) MessageType {
	if msg == MsgAppend {
		return MsgAppendReply
	} else if msg == MsgHeartbeat {
		return MsgHeartbeatReply
	}
	return MsgStop
}

func (rf *Raft) maybeApply() {
	if rf.raftLog.applied < rf.raftLog.commited && rf.raftLog.commited < len(rf.raftLog.Entries) {
		for _, e := range rf.raftLog.Entries[rf.raftLog.applied+1 : rf.raftLog.commited+1] {
			m := rf.createApplyMsg(e)
			if m.CommandValid {
				fmt.Printf("%d reply an entry of log[%d]=%d\n", rf.me, m.CommandIndex, m.Command.(int))
			}
			rf.applySM <- rf.createApplyMsg(e)
		}
		rf.raftLog.applied = rf.raftLog.commited
	}
}

