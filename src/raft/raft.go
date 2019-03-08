package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"bytes"
	"fmt"
	"labgob"
	"sync/atomic"
	"sort"
	"math/rand"
)
// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


//
// A Go object implementing a single Raft peer.
//

//
// A Go object implementing a single Raft peer.
//

type RoleState int
const (
	_ RoleState = iota
	Leader
	Candidate
	PreCandidate
	Follower
)


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	clients		[]RaftClient

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term	  int
	vote 	  int
	leader    int
	state	  RoleState
	prevState HardState
	electionTimeout int32
	rdElectionTimeout int32
	lastHeartBeat int32
	lastElection time.Time
	applySM    chan ApplyMsg
	msgChan    chan AppendMessage
	replyChan  chan AppendReply
	raftLog	  UnstableLog
	votes	  []int
	stop 		int32
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) reset(term int)  {
	rf.term = term
	for idx := range rf.votes {
		rf.votes[idx] = -1
	}
	rf.lastHeartBeat = 0
	rf.lastElection = time.Now()
	rf.vote = -1
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d Get term: %d,  state: %d\n", rf.me, rf.term, rf.state)
    return rf.term, rf.state == Leader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.raftLog.commited)
	e.Encode(rf.raftLog.Entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	fmt.Printf("%d save to %d, %d, %d\n", rf.me, rf.term, rf.vote, rf.raftLog.commited)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.vote)
	d.Decode(&rf.raftLog.commited)
	d.Decode(&rf.raftLog.Entries)
	rf.raftLog.applied = 0
	for idx, e := range rf.raftLog.Entries {
		if idx > rf.raftLog.commited {
			break
		}
		m := rf.createApplyMsg(e)
		if m.CommandValid {
			rf.raftLog.pk = m.CommandIndex
		}
	}
	fmt.Printf("%d recover from %d, %d, %d\n", rf.me, rf.term, rf.vote, rf.raftLog.commited)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.maybeChange()
	reply.To = rf.me
	fmt.Printf("%d(%d) AccessRequest(%s) vote from %d(%d)\n", rf.me, rf.term, getMsgName(args.MsgType), args.From, args.Term)
	if !rf.checkVote(args.From, args.Term, args.MsgType, &reply.VoteGranted) || rf.state == Leader {
		reply.Term = rf.term
		fmt.Printf("%d %d reject smaller term: %d\n", rf.me, rf.term, args.Term)
		return
	}
	if ((rf.leader == -1 && rf.vote == -1) || rf.vote == args.From ||
		(args.MsgType == MsgRequestPrevote && rf.term < args.Term)) &&
		rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%d (leader:%d, vote: %d, state: %d) agree vote for: %d\n", rf.me, rf.leader,
			rf.vote, rf.state, args.From)
		reply.VoteGranted = true
		reply.Term = args.Term
		if args.MsgType == MsgRequestVote {
			rf.vote = args.From
			rf.lastElection = time.Now()
		}
		return
	}
	fmt.Printf("%d reject vote for: %d, leader: %d, vote: %d\n", rf.me, args.From, rf.leader, rf.vote)
	reply.VoteGranted = false
	reply.Term = rf.term
}

func (rf *Raft) AppendEntries(args *AppendMessage, reply* AppendReply) {
	rf.mu.Lock()
	reply.To = args.From
	reply.From = rf.me
	reply.MsgType = getResponseType(args.MsgType)
	reply.Id = args.Id

	if !rf.checkAppend(args.From, args.Term, args.MsgType) {
		fmt.Printf("%d reject (%s) from leader: %d, term: %d, leadder term: %d\n", rf.me, getMsgName(args.MsgType),
			args.From, rf.term, args.Term)
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = 0
		rf.mu.Unlock()
		return
	}
	rf.leader = args.From
	rf.lastElection = time.Now()
	if args.MsgType == MsgHeartbeat {
		fmt.Printf("%d(commit: %d, applied: %d, total: %d) access Heartbeat from %d(%d) to %d\n", rf.me, rf.raftLog.commited,
			rf.raftLog.applied, len(rf.raftLog.Entries), args.From, args.Commited, args.To)
		rf.handleHeartbeat(args, reply)
	} else {
		rf.handleAppendEntries(args, reply)
		fmt.Printf("%d(%d) access append from %d(%d) to %d\n", rf.me, rf.raftLog.commited,
			args.From, args.Commited, args.To)
	}
	if rf.raftLog.applied < rf.raftLog.commited && rf.raftLog.commited < len(rf.raftLog.Entries) {
		for _, e := range rf.raftLog.Entries[rf.raftLog.applied+1 : rf.raftLog.commited+1] {
			m := rf.createApplyMsg(e)
			if m.CommandValid {
				fmt.Printf("%d apply an entry of log[%d]=data[%d]=%d\n", rf.me, e.Index, m.CommandIndex, m.Command.(int))
				rf.applySM <- rf.createApplyMsg(e)
			}
		}
		rf.raftLog.applied = rf.raftLog.commited
	}
	rf.mu.Unlock()
	rf.maybeChange()
}

func (rf *Raft) AppendEntriesResponse(args *AppendReply, done* DoneReply) {
	if atomic.LoadInt32(&rf.stop) != 1 {
		rf.replyChan <- *args
	}
}

func (rf *Raft) handleHeartbeat(msg *AppendMessage, reply *AppendReply)  {
	reply.Success = true
	reply.Term = MaxInt(rf.term, reply.Term)
	reply.Commited = len(rf.raftLog.Entries)
	reply.MsgType = MsgHeartbeatReply
	rf.term = msg.Term
	if rf.raftLog.MaybeCommit(msg.Commited) {
		fmt.Printf("%d commit to %d, log length: %d, last index:%d leader : %d\n",
			rf.me, rf.raftLog.commited, len(rf.raftLog.Entries), rf.raftLog.GetLastIndex(), msg.From)
	}
}

func (rf *Raft) handleAppendEntries(args *AppendMessage, reply *AppendReply)  {
	reply.MsgType = MsgAppendReply
	index := len(rf.raftLog.Entries)
	if args.PrevLogIndex >= index {
		fmt.Printf("%d : %d(index: %d, %d) reject append entries from %d(prev index: %d)\n",
			args.Id, rf.me, index - 1, rf.term, args.From, args.PrevLogIndex)
		reply.Success = false
		reply.Commited = index - 1
		reply.Term = rf.term
		return
	}
	if rf.raftLog.Entries[args.PrevLogIndex].Term == args.PrevLogTerm {
		lastIndex := args.PrevLogIndex
		for idx, e := range args.Entries {
			if e.Index != idx + args.PrevLogIndex + 1{
				fmt.Printf("%d(index: %d) =====append error entries from %d(prev index: %d)\n",
					rf.me, index - 1, args.From, args.PrevLogIndex)
				return
			}
			e.Index = idx + args.PrevLogIndex + 1
			e.Term = args.Term
			if e.Index < rf.raftLog.commited {
				continue
			}
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
		rf.raftLog.MaybeCommit(MinInt(args.Commited, lastIndex))
		reply.Term = rf.term
		reply.Commited = lastIndex
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = args.PrevLogIndex - 1
		e := rf.raftLog.Entries[args.PrevLogIndex]
		fmt.Printf("%d(index: %d, term: %d) %d reject append entries from %d(prev index: %d, term: %d)\n",
			rf.me, e.Index, e.Term, rf.raftLog.commited, args.From, args.PrevLogIndex, args.PrevLogTerm)
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

func (rf *Raft) handleAppendReply(reply* AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d handleAppendReply from %d at\n", rf.me, reply.From)
	if !rf.checkAppend(reply.From, reply.Term, reply.MsgType) {
		return
	}
	if rf.leader != rf.me {
		return
	}
	pr := &rf.clients[reply.From]
	pr.active = true
	if reply.MsgType == MsgHeartbeatReply {
		if pr.matched < rf.raftLog.GetLastIndex() {
			rf.appendMore(reply.From)
		}
		fmt.Printf("%d access  HeartbeatReply from %d(matched: %d, %d)\n", rf.me, reply.From,
			pr.matched, rf.raftLog.GetLastIndex())
		return
	}
	if !reply.Success {
		fmt.Printf("%d(%d) handleAppendReply failed, from %d(%d). send again since %d\n",
			rf.me, rf.term, reply.From, reply.Term, reply.Commited + 1)
		//pr.matched --
		pr.next = reply.Commited + 1
		rf.appendMore(reply.From)
	} else {
		fmt.Printf("%d: %d handleAppendReply from %d(%d), commit log from %d to %d\n",
			reply.Id, rf.me, reply.From, reply.Term, pr.matched, reply.Commited)

		if pr.matched < reply.Commited {
			pr.matched = reply.Commited
			pr.next = reply.Commited + 1
		}
/*		if reply.Commited <= rf.raftLog.commited {
			return
		}*/
		commits := make([]int, len(rf.peers))
		for i, p := range rf.clients {
			if i == rf.me {
				commits[i] = len(rf.raftLog.Entries) - 1
			} else {
				commits[i] = p.matched
			}
		}
		sort.Ints(commits)
		quorum := len(rf.peers) / 2
		fmt.Printf("%d receive a msg commit : %d from %d\n", rf.me, reply.Commited, reply.From)
		fmt.Printf("%d commit %d, to commit %d, apply %d, all: %d\n",
			rf.me, rf.raftLog.commited, commits[quorum], rf.raftLog.applied,
			len(rf.raftLog.Entries))
		if rf.raftLog.commited < commits[quorum] {
			rf.raftLog.commited = commits[quorum]
			for _, e := range rf.raftLog.GetUnApplyEntry() {
				m := rf.createApplyMsg(e)
				if e.Index != rf.raftLog.applied + 1 {
					fmt.Printf("%d APPLY ERROR! %d, %d\n", rf.me, e.Index, rf.raftLog.applied)
				}
				rf.raftLog.applied += 1
				if m.CommandValid {
					rf.applySM <- m
					fmt.Printf("%d apply a message of log[%d]=data[%d] = %d\n", rf.me, e.Index, m.CommandIndex, m.Command.(int))
				}
			}
			fmt.Printf("%d apply message\n", rf.me)
		}
		fmt.Printf("%d send handleAppendReply end\n", rf.me)
		rf.maybeChange()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(args RequestVoteArgs) bool {
	if args.Term < rf.term {
		fmt.Printf("failed to send request vote from %d to %d \n", args.From, args.To)
		return true
	}
	var reply RequestVoteReply
	if args.MsgType == MsgRequestPrevote {
		reply.MsgType = MsgRequestPrevoteReply
	} else {
		reply.MsgType = MsgRequestVoteReply
	}
	fmt.Printf("begin send request vote from %d to %d \n", args.From, args.To)
	ok := rf.peers[args.To].Call("Raft.RequestVote", &args, &reply)
	reply.To = args.To
	if ok {
		//fmt.Printf("send request vote from %d to %d ok\n", args.From, args.To)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//fmt.Printf("send request vote from %d to %d get lock\n", args.From, args.To)
		fmt.Printf("send request vote from %d to %d \n", args.From, args.To)
		rf.handleVoteReply(&reply)
	}
	//fmt.Printf("send request vote from %d to %d at %d, result: %t\n", args.From, args.To, ts, ok)
	return ok
	//ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) handleVoteReply(reply* RequestVoteReply) {
	fmt.Printf("%d(%d): receive vote reply from %d(%d), state: %d\n",
		rf.me, rf.term, reply.To, reply.Term, rf.state)
	defer rf.maybeChange()
	if !rf.checkVote(reply.To, reply.Term, reply.MsgType, &reply.VoteGranted) {
		return
	}
	if (rf.state == Candidate && reply.MsgType == MsgRequestVoteReply) ||
		(rf.state == PreCandidate && reply.MsgType == MsgRequestPrevoteReply) {
		fmt.Printf("%d(%d): access vote reply from %d(%d), accept: %t, state: %d\n",
			rf.me, rf.term, reply.To, reply.Term, reply.VoteGranted, rf.state)
		if reply.VoteGranted {
			rf.votes[reply.To] = 1
		} else {
			rf.votes[reply.To] = 0
		}
		quorum := len(rf.peers) / 2 + 1
		accept := 0
		reject := 0
		for _, v := range rf.votes {
			if v == 1 {
				accept += 1
			} else if v == 0 {
				reject += 1
			}
		}
		if accept >= quorum {
			for idx, v := range rf.votes {
				if v == 1 {
					fmt.Printf("%d vote for me(%d).\n", idx, rf.me)
				}
			}
			fmt.Printf("%d win.\n", rf.me)
			if rf.state == PreCandidate {
				fmt.Printf("%d win prevote\n", rf.me)
				rf.campaign(MsgRequestVote)
				rf.mu.Lock()
			} else {
				fmt.Printf("%d win vote\n", rf.me)
				rf.becomeLeader()
				rf.propose([]byte{})
			}
		} else if reject == quorum {
			fmt.Printf("%d has been reject by %d members\n", rf.me, reject)
			rf.becomeFollower(rf.term, -1)
		}
	}
	fmt.Printf("%d(%d): receive vote end\n", rf.me, rf.term)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.leader != rf.me {
		return len(rf.raftLog.Entries), rf.term, false
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	index := rf.raftLog.pk + 1
	rf.raftLog.pk += 1
	e.Encode(index)
	e.Encode(command)
	data := w.Bytes()
	rf.propose(data)
	logIndex := rf.raftLog.GetLastIndex()
	rf.mu.Unlock()
	//accept := rf.sendAppendEntries()
	for t := 0; t < 20; t ++ {
		time.Sleep(time.Millisecond * 100)
		if rf.raftLog.applied >= logIndex {
			fmt.Printf("%d Store a message of %d in %d,(term: %d, logIndex: %d)\n",
				rf.me, command.(int), index, rf.term, logIndex)
			return index, rf.term, true
		}
	}
	fmt.Printf("%d Store a message of %d timeout in CommandIndex %d, logIndex: %d\n",
		rf.me, command.(int), index, logIndex)
	return index, rf.term, true
}

func (rf *Raft) createApplyMsg(e Entry) ApplyMsg {
	var applyMsg ApplyMsg
	var index int
	var tmp int
	if len(e.Data) > 0 {
		r := bytes.NewBuffer(e.Data)
		d := labgob.NewDecoder(r)
		d.Decode(&index)
		d.Decode(&tmp)
		applyMsg.CommandIndex = index
		applyMsg.Command = tmp
		applyMsg.CommandValid = true
		//fmt.Printf("%d Apply entre : term: %d, index: %d, value : %d\n", rf.me, e.Term, applyMsg.CommandIndex, tmp)
	} else {
		applyMsg.Command = -1
		applyMsg.CommandValid = false
		//applyMsg.CommandValid = false
		fmt.Printf("%d empty Apply entre : term: %d, index: %d, value\n", rf.me, e.Term, e.Index)
	}
	return applyMsg
}

func MaxInt(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}


func (rf *Raft) sendReply(reply *AppendReply) {
	var done DoneReply
	rf.peers[reply.To].Call("Raft.AppendEntriesResponse", reply, &done)
}

func (rf *Raft) appendMore(idx int) {
	msg := rf.createMessage(idx, MsgAppend)
	msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(rf.clients[idx].next)
	fmt.Printf("%d send again handleAppendReply to %d since %d, which matched %d\n", rf.me, idx, msg.PrevLogIndex, rf.clients[idx].matched)
	msg.PrevLogTerm = rf.raftLog.Entries[msg.PrevLogIndex].Term
	rf.clients[msg.To].Send(msg)
}

func (rf *Raft) checkAppend(from int, term int, msgType MessageType) bool {
	if term > rf.term {
		rf.becomeFollower(term, from)
		//return false
	} else if term < rf.term {
		fmt.Printf("==================!ERROR!======append message(%s) from %d(%d) to %d(%d) can not be reach, leader: %d\n",
			getMsgName(msgType), from, term, rf.me, rf.term, rf.leader)
		return false
	}
	return true
}

func (rf *Raft) checkVote(from int, term int, msgType MessageType, accept* bool) bool {
	if term > rf.term {
		if msgType == MsgRequestVote || msgType == MsgRequestPrevote{
			if rf.passed_election_time(rf.electionTimeout, time.Now()) && rf.leader != -1 {
				fmt.Printf("%d(%d) reject a msg (%s) from %d, term:%d. leader %d\n",
					rf.me, rf.term, getMsgName(msgType), from, term, rf.leader)
				*accept = false
				return false
			}
		}
		fmt.Printf("%d(%d) receive a larger term(%d) from %d of %s, current leader: %d\n",
			rf.me, rf.term, term, from, getMsgName(msgType), rf.leader)
		if msgType == MsgRequestPrevote || (msgType == MsgRequestPrevoteReply && *accept == true) {

		} else {
			rf.becomeFollower(term, -1)
		}
	} else if term < rf.term && msgType == MsgRequestPrevote {
		//if msgType == MsgAppend || msgType == MsgHeartbeat
		*accept = false
		return false
	}
	return true;
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//rf.stop <- true
	//rf.propose <- MsgStop
	atomic.StoreInt32(&rf.stop, 1)
	time.Sleep(100 * time.Millisecond)
	for idx := range rf.clients {
		if idx != rf.me {
			rf.clients[idx].Stop()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) becomeFollower(term int, leader int) {
	rf.reset(term)
	rf.state = Follower
	rf.leader = leader
	fmt.Printf("%d become follower of %d in term: %d\n", rf.me, leader, term)
}

func (rf *Raft) becomeLeader() {
	index := rf.raftLog.GetLastIndex()
	for idx := range rf.clients {
		pr := &rf.clients[idx]
		pr.next = index + 1
		pr.active = false
		if idx == rf.me {
			pr.matched = index
		} else{
			pr.matched = 0
		}
	}
	fmt.Printf("%d become leader\n", rf.me)
	//time.Sleep(10 * time.Millisecond)
	rf.state = Leader
	rf.leader = rf.me
	rf.lastHeartBeat = 0
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeCandidate(msgType MessageType) int {
	term := rf.term + 1
	if msgType == MsgRequestPrevote {
		rf.state = PreCandidate
	} else {
		rf.reset(rf.term + 1)
		rf.state = Candidate
		rf.votes[rf.me] = 1
		rf.vote = rf.me
	}
	fmt.Printf("%d become %s candidate, %v\n", rf.me, getMsgName(msgType), rf.lastElection)
	return term
}

func (rf *Raft) getUnsendEntries(since int) ([]Entry, int) {
	if since >= len(rf.raftLog.Entries) {
		return []Entry{}, since - 1
	}
	Entries := rf.raftLog.Entries[since:]
	return Entries, since - 1
}

func (rf *Raft) createMessage(to int, msgType MessageType) AppendMessage {
	var msg AppendMessage
	msg.Term = rf.term
	msg.From = rf.me
	msg.To = to
	msg.MsgType = msgType
	return msg
}

type Pair struct {
	value int
	idx	  int
}

type Pairs []Pair

func (p Pairs) Len() int {
	return len(p)
}

func (p Pairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type SortByFirst struct { Pairs }

func (p SortByFirst) Less(i, j int) bool {
	return p.Pairs[i].value > p.Pairs[j].value
}

func (rf *Raft) propose(data []byte) {
	logNum := len(rf.raftLog.Entries)
	rf.raftLog.Entries = append(rf.raftLog.Entries,
		Entry{data, rf.term, logNum})
	rf.broadcast()
}

func (rf *Raft) broadcast() {
	fmt.Printf("%d: BeginSend append entries\n", rf.me)
	defer fmt.Printf("%d: EndSend append entries:\n", rf.me)
	msg := rf.createMessage(0, MsgAppend)
	for id, pr := range rf.clients {
		if id != int(rf.me) {
			msg.To = id
			msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(pr.next)
			if len(msg.Entries) == 0 {
				continue
			}
			msg.Commited = MinInt(rf.raftLog.commited, pr.matched)
			fmt.Printf("%d: broadcast append to %d since %d\n", rf.me, id, pr.next)
			msg.PrevLogTerm = rf.raftLog.Entries[msg.PrevLogIndex].Term
			rf.clients[msg.To].Send(msg)
		}
	}
	rf.lastHeartBeat = 0
}

func (rf *Raft) bcastHeartbeat(msg AppendMessage) {
	for idx, pr := range rf.clients {
		if idx != rf.me {
			msg.To = idx
			msg.Commited = MinInt(pr.matched, rf.raftLog.commited)
			fmt.Printf("%d: broadcast heartbeat to %d, commit to min(%d, %d)\n", rf.me, idx, pr.matched, rf.raftLog.commited)
			rf.clients[msg.To].Send(msg)
		}
	}
}


func (rf *Raft) maybeLose() {
	succeed := 0
	for idx, v := range rf.clients {
		if idx == rf.me {
			succeed ++
		} else if v.active {
			succeed ++
			rf.clients[idx].active = false
		} else {
			fmt.Printf("%d lose contact of %d.\n", rf.me, idx)
		}
	}
	if succeed <= len(rf.clients) / 2 {
		rf.becomeFollower(rf.term, -1)
	}
}

func (rf *Raft) maybeChange() {
	state := HardState{rf.term, rf.vote, rf.raftLog.commited}
	if state != rf.prevState {
		rf.persist()
		rf.prevState = state
	}
}

func (rf *Raft) campaign(msgType MessageType) {
	fmt.Printf("%d begin %s campagin at term:%d, state:%d\n", rf.me, getMsgName(msgType), rf.term, rf.state)
	term := rf.becomeCandidate(msgType)
	rf.votes[rf.me] = 1
	lastLogIndex := rf.raftLog.GetLastIndex()
	lastLogTerm := rf.raftLog.GetLastTerm()
	rf.maybeChange()
	rf.mu.Unlock()

	for idx, _ := range rf.peers {
		if idx != rf.me {
			var msg RequestVoteArgs
			msg.MsgType = msgType
			msg.From = rf.me
			msg.Term = term
			msg.LastLogIndex = lastLogIndex
			msg.LastLogTerm = lastLogTerm
			msg.To = idx
			go rf.sendRequestVote(msg)
			//rf.argsChan <- msg
		}
	}
}

func (rf *Raft) passed_election_time(electionTimeout int32, now time.Time) bool {
	return rf.lastElection.Add(time.Duration(electionTimeout) * time.Millisecond).Before(now)
}

func (rf *Raft) step() {
	for atomic.LoadInt32(&rf.stop) != 1{
		rf.mu.Lock()
		now := time.Now()
		if rf.state == Leader {
			if rf.lastHeartBeat > 200 {
				rf.lastHeartBeat = 0
				msg := rf.createMessage(0, MsgHeartbeat)
				// unlock in broad heartbeat
				rf.mu.Unlock()
				rf.bcastHeartbeat(msg)
				continue
			} else if rf.passed_election_time(rf.electionTimeout, now) {
				//rf.resetElection(now)
				rf.lastElection = now
				rf.maybeLose()
				rf.maybeChange()
			}
		} else if rf.passed_election_time(rf.rdElectionTimeout, now) {
			rf.lastElection = now
			rf.campaign(MsgRequestPrevote)
			continue
		}
		rf.lastHeartBeat += 50
		rf.mu.Unlock()
		//fmt.Printf("Sleep 50ms\n");
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
	fmt.Printf("Stop Raft: %d\n", rf.me)
}

var electionTimes = make(map[int32]bool)
//var eletionTimes [2000]bool

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Printf("%d : start a Raft instance\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	e := Entry{[]byte{}, 0, 0}
	rf.raftLog = UnstableLog{
		[]Entry{e},
		0, 0, 0,
	}
	rf.prevState = HardState{0, -1, 0}
	rf.term = 0
	rf.vote = -1
	rf.rdElectionTimeout = int32(100 + rand.Intn(20) * 200)
	//rf.rdElectionTimeout = 1000
	time.Sleep(time.Duration(rf.rdElectionTimeout) * time.Millisecond)
	rf.electionTimeout = 900
	rf.rdElectionTimeout = int32(900 + rand.Intn(7) * 80)
	for  {
		if _, ok := electionTimes[rf.rdElectionTimeout]; ok {
			//rf.rdElectionTimeout += 100
			rf.rdElectionTimeout = int32(900 + rand.Intn(7) * 80)
		} else {
			break
		}
	}
	electionTimes[rf.rdElectionTimeout] = true
	rf.lastHeartBeat = 0
	rf.lastElection = time.Now()
	rf.stop = 0
	rf.applySM = applyCh
	rf.msgChan = make(chan AppendMessage, 1000)
	rf.replyChan = make(chan AppendReply, 1000)
	rf.votes = make([]int, len(rf.peers))
	// Your initialization code here.
	rf.becomeFollower(0, -1)
	rf.readPersist(persister.ReadRaftState())
	rf.clients = make([]RaftClient, len(rf.peers))
	for idx := range rf.clients {
		if idx != rf.me {
			rf.clients[idx].id = idx
			rf.clients[idx].peer = rf.peers[idx]
			rf.clients[idx].raft= rf
			rf.clients[idx].Start()
		}
	}

	go rf.step()
	fmt.Printf("%d : random election timeout: %d\n", rf.me, rf.rdElectionTimeout)
	return rf
}
