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
	"math/rand"
	"time"
	"bytes"
	"fmt"
	"labgob"
	"sort"
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
type MessageType int
const (
	_ MessageType = iota
	MsgStop
	MsgPropose
	MsgHeartbeat
	MsgAppendReply
	MsgAppend
	MsgRequestVote
)

type AppendMessage struct {
	MsgType			MessageType
	Term			int
	From			int
	To 				int
	Commited		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]Entry
}

type AppendReply struct {
	Term        int
	To 			int
	Commited	int
	Success		bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	From			int
	To 				int
	LastLogIndex 	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	To 			int
	VoteGranted	bool
}

//
// A Go object implementing a single Raft peer.
//

type RoleState int
const (
	_ RoleState = iota
	Leader
	Candidate
	Follower
)


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term	  int
	vote 	  int
	leader    int
	state	  RoleState
	rdElectionTimeout int32
	lastHeartBeat int32
	lastElection int32
	applySM    chan ApplyMsg
	msgChan    chan AppendMessage
	argsChan   chan RequestVoteArgs
	raftLog	  UnstableLog
	votes	  []int
	nextIndex []int
	matchIndex []int
	actives		[]bool
	ts 			int
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) reset(term int)  {
	rf.term = term
	for idx := range rf.votes {
		rf.votes[idx] = -1
	}
	rf.lastHeartBeat = 0
	rf.lastElection = 0
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d Get term: %d,  state: %d, ts: %d\n", rf.me, rf.term, rf.state, rf.ts)
    return rf.term, rf.state == Leader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	e.Encode(rf.raftLog)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	d.Decode(rf.raftLog)
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
	fmt.Printf("%d(%d) AccessRequest vote for: %d(%d) at %d\n", rf.me, rf.term, args.From, args.Term, rf.ts)
	if !rf.checkTerm(args.From, args.Term, MsgRequestVote) {
		reply.VoteGranted = false
		reply.Term = rf.term
		reply.To = rf.me
		fmt.Printf("%d %d reject smaller term: %d\n", rf.me, rf.term, args.Term)
		return
	}
	if ((rf.leader == -1 && rf.vote == -1) || rf.vote == args.From || rf.term < args.Term) &&
		rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%d agree vote for: %d\n", rf.me, args.From)
		rf.vote = args.From
		rf.becomeFollower(args.Term, -1)
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}
	if rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%d(%d) reject vote for: %d(%d), has voted to %d\n", rf.me, rf.term, args.From, args.Term, rf.vote)
	} else {
		fmt.Printf("%d(%d) reject vote for: %d(%d), self(%d, %d), msg(%d,%d)\n",
			rf.me, rf.term, args.From, args.Term, rf.raftLog.GetLastIndex(), rf.raftLog.GetLastTerm(),
			args.LastLogIndex, args.LastLogTerm)
	}
	reply.VoteGranted = false
	reply.Term = args.Term
}

func (rf *Raft) AppendEntries(args *AppendMessage, reply *AppendReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d(%d) access append entries from %d(%d)\n", rf.me, rf.term, args.From, args.Term)
	if !rf.checkTerm(args.From, args.Term, args.MsgType) {
		reply.Success = false
		reply.To = rf.me
		rf.term = 0
		reply.Commited = 0
		return
	}

	defer fmt.Printf("%d: EndAppendEntries \n", rf.me, )

	index := len(rf.raftLog.Entries)
	if args.PrevLogIndex < rf.raftLog.commited {
		fmt.Printf("%d(index: %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, args.From, args.PrevLogIndex)
		reply.Success = true
		reply.Commited = rf.raftLog.commited
		reply.To = rf.me
		reply.Term = rf.term
		return
	} else if args.PrevLogIndex >= index {
		fmt.Printf("%d(index: %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, args.From, args.PrevLogIndex)
		reply.Success = false
		reply.Commited = rf.raftLog.commited
		reply.To = rf.me
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
		if rf.raftLog.applied < rf.raftLog.commited && rf.raftLog.commited < len(rf.raftLog.Entries) {
			for _, e := range rf.raftLog.Entries[rf.raftLog.applied + 1 : rf.raftLog.commited + 1] {
				//if len(e.Data) > 0 {
				rf.applySM <- rf.createApplyMsg(e)
				//}
			}
		}
		rf.raftLog.applied = rf.raftLog.commited
		reply.Term = rf.term
		reply.Commited = lastIndex
		reply.To = rf.me
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = index - 1
		e := rf.raftLog.Entries[args.PrevLogIndex]
		fmt.Printf("%d(index: %d, term: %d) reject append entries from %d(prev index: %d, term: %d)\n",
			rf.me, e.Index, e.Term, args.From, args.PrevLogIndex, args.PrevLogTerm)
	}
}

func (rf *Raft) HeartBeat(msg *AppendMessage, reply *AppendReply)  {
	if !rf.checkTerm(msg.From, msg.Term, msg.MsgType) {
		reply.Success = false
		reply.To = rf.me
		reply.Term = 0
		reply.Commited = 0
		return
	}
	reply.Success = true
	reply.To = rf.me
	reply.Term = MaxInt(rf.term, reply.Term)
	rf.lastElection = 0
	rf.term = msg.Term
	if msg.Commited > rf.raftLog.commited {
		rf.raftLog.commited = msg.Commited
	}
	return
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
		return true
	}
	var reply RequestVoteReply
	ts := rf.ts
	ok := rf.peers[args.To].Call("Raft.RequestVote", &args, &reply)
	reply.To = args.To
	if ok {
		//fmt.Printf("send request vote from %d to %d ok\n", args.From, args.To)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//fmt.Printf("send request vote from %d to %d get lock\n", args.From, args.To)
		fmt.Printf("send request vote from %d to %d  at %d\n", args.From, args.To, ts)
		ts = rf.ts
		rf.handleVoteReply(&reply)
	}
	//fmt.Printf("send request vote from %d to %d at %d, result: %t\n", args.From, args.To, ts, ok)
	return ok
	//ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) handleVoteReply(reply* RequestVoteReply) {
	fmt.Printf("%d(%d): receive vote reply from %d(%d), state: %d\n",
		rf.me, rf.term, reply.To, reply.Term, rf.state)
	if !rf.checkTerm(reply.To, reply.Term, MsgAppendReply) {
		return
	}
	if rf.state == Candidate {
		fmt.Printf("%d(%d): access vote reply from %d(%d), accept: %t at %d, state: %d\n",
			rf.me, rf.term, reply.To, reply.Term, reply.VoteGranted, rf.ts, rf.state)
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
			// become leader
			rf.becomeLeader()
			rf.propose([]byte{})
		} else if reject > len(rf.peers) - quorum {
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
	rf.mu.Unlock()
	//rf.msgChan <- msg
	//accept := rf.sendAppendEntries()
	for t := 0; t < 20; t ++ {
		time.Sleep(time.Millisecond * 100)
		if rf.raftLog.applied >= index {
			fmt.Printf("%d Store a message of %d in %d,(term: %d)\n", rf.me, command.(int), index, rf.term)
			return index, rf.term, true
		}
	}
	fmt.Printf("%d Store a message of %d timeout\n", rf.me, command.(int))
	return index, rf.term, rf.state == Leader
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
		fmt.Printf("%d Apply entre : term: %d, index: %d, value : %d\n", rf.me, e.Term, applyMsg.CommandIndex, tmp)
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

func (rf *Raft) sendAppendEntries(msg AppendMessage) bool {
	var reply AppendReply
	ts := rf.ts
	ok := rf.peers[msg.To].Call("Raft.AppendEntries", &msg, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		ts = rf.ts
		fmt.Printf("send append msg from %d to %d, at %d\n", msg.From, msg.To, ts)
		rf.handleAppendReply(&reply)
	}
	fmt.Printf("AppendEntries from %d to %d, %t, at %d\n", msg.From, msg.To, ok, ts)
	return ok
}

func (rf *Raft) handleAppendReply(reply* AppendReply) {
	fmt.Printf("%d handleAppendReply from %d at %d\n", rf.me, reply.To, rf.ts)
	if !rf.checkTerm(reply.To, reply.Term, MsgAppendReply) || rf.state != Leader {
		return
	}
	rf.actives[reply.To] = true
	if !reply.Success {
		fmt.Printf("%d(%d) handleAppendReply failed, from %d(%d)\n", rf.me, rf.term, reply.To, reply.Term)
		rf.nextIndex[reply.To] --
		msg := rf.createMessage(reply.To, MsgAppend)
		msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(reply.To)
		fmt.Printf("%d send again handleAppendReply since %d\n", rf.me, msg.PrevLogIndex)
		msg.PrevLogTerm = rf.raftLog.Entries[msg.PrevLogIndex].Term
		rf.msgChan <- msg
	} else {
		if rf.matchIndex[reply.To] < reply.Commited {
			rf.matchIndex[reply.To] = reply.Commited
			rf.nextIndex[reply.To] = reply.Commited + 1
		}
/*		if reply.Commited <= rf.raftLog.commited {
			return
		}*/
		commits := make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			if i == rf.me {
				commits[i] = len(rf.raftLog.Entries) - 1
			} else {
				commits[i] = rf.matchIndex[i]
			}
		}
		sort.Ints(commits)
		quorum := len(rf.peers) / 2
		fmt.Printf("%d receive a msg commit : %d from %d\n", rf.me, reply.Commited, reply.To)
		fmt.Printf("%d commit %d, to commit %d, apply %d, all: %d\n",
			rf.me, rf.raftLog.commited, commits[quorum], rf.raftLog.applied,
			len(rf.raftLog.Entries))
		if rf.raftLog.commited < commits[quorum] {
			rf.raftLog.commited = commits[quorum]
			for _, e := range rf.raftLog.GetUnApplyEntry() {
				rf.applySM <- rf.createApplyMsg(e)
				if e.Index != rf.raftLog.applied + 1 {
					fmt.Printf("%d APPLY ERROR! %d, %d\n", rf.me, e.Index, rf.raftLog.applied)
				}
				rf.raftLog.applied += 1
			}
			rf.broadcast()
			fmt.Printf("%d apply message\n", rf.me)
		}
		fmt.Printf("%d send handleAppendReply end\n", rf.me)
	}
}

func (rf *Raft) checkTerm(from int, term int, msgType MessageType) bool {
	if term > rf.term {
		if msgType == MsgRequestVote {
			if rf.lastElection < rf.rdElectionTimeout && rf.leader != -1 {
				return false;
			}
		} else {
			fmt.Printf("%d(%d) receive a larger term(%d) from %d of %d\n", rf.me, rf.term, term, from, msgType)
			if msgType == MsgAppend || msgType == MsgHeartbeat{
				rf.becomeFollower(term, from)
			} else {
				rf.becomeFollower(term, -1)
			}
		}
	} else if (term < rf.term) {
		//if msgType == MsgAppend || msgType == MsgHeartbeat
		return false
	}
	return true
}

func (rf *Raft) sendHeartbeat(msg AppendMessage) {
	var reply AppendReply
	ts := rf.ts
	reply.To = msg.To
	ok := rf.peers[msg.To].Call("Raft.HeartBeat", &msg, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		ts = rf.ts
		fmt.Printf("send heartbeat msg from %d to %d, at %d\n", msg.From, msg.To, ts)
		if msg.Term == rf.term {
			rf.actives[msg.To] = true
		}
		rf.checkTerm(msg.To, reply.Term, MsgAppendReply)
	}
	//fmt.Printf("heart from %d to %d, %t, at %d\n", msg.From, msg.To, ok, ts)
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
	//close(rf.msgChan)
	//close(rf.argsChan)
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
	fmt.Printf("%d become follower of %d in term: %d when %d\n", rf.me, leader, term, rf.ts)
}

func (rf *Raft) becomeLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	index := rf.raftLog.GetLastIndex()
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = index + 1
		if idx != rf.me {
			rf.matchIndex[idx] = index
		} else{
			rf.matchIndex[idx] = 0
		}
	}
	rf.state = Leader
	rf.leader = rf.me
	rf.lastHeartBeat = 0
	rf.lastElection = 0
	fmt.Printf("%d become leader when %d\n", rf.me, rf.ts)
}

func (rf *Raft) becomeCandidate() {
	rf.leader = -1
	rf.state = Candidate
	rf.reset(rf.term + 1)
	rf.vote = rf.me
	close(rf.argsChan)
	rf.argsChan = make(chan RequestVoteArgs, 10000)
	fmt.Printf("%d become candidate when %d\n", rf.me, rf.ts)
}

func (rf *Raft) getUnsendEntries(idx int) ([]Entry, int) {
	if rf.nextIndex[idx] >= len(rf.raftLog.Entries) {
		return []Entry{}, rf.nextIndex[idx] - 1
	}
	Entries := rf.raftLog.Entries[rf.nextIndex[idx]:]
	return Entries, rf.nextIndex[idx] - 1
}

func (rf *Raft) createMessage(to int, msgType MessageType) AppendMessage {
	var msg AppendMessage
	msg.Term = rf.term
	msg.From = rf.me
	msg.To = to
	msg.Commited = rf.raftLog.commited
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
	for id, _ := range rf.peers {
		if id != int(rf.me) {
			msg.To = id
			msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(id)
			msg.PrevLogTerm = rf.raftLog.Entries[msg.PrevLogIndex].Term
			//go rf.sendAppendEntries(msg)
			rf.msgChan <- msg
		}
	}
}

func (rf *Raft) leaderBroadCast() {
	fmt.Printf("%d: HeartBeat! %d\n", rf.me, rf.ts)
	defer fmt.Printf("%d: EndHeartBeat! %d\n", rf.me, rf.ts)
	msg := rf.createMessage(0, MsgHeartbeat)
	//rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx != rf.me {
			msg.To = idx
			//time.Sleep(10 * time.Millisecond)
			//go rf.sendHeartbeat(msg)
			rf.msgChan <- msg
		}
	}
}


func (rf *Raft) maybeLose() {
	succeed := 0
	for idx, v := range rf.actives {
		if idx == rf.me {
			succeed ++
		} else if v {
			succeed ++
			rf.actives[idx] = false
		} else {
			fmt.Printf("%d lose contact of %d.\n", rf.me, idx)
		}
	}
	if succeed <= len(rf.actives) / 2 {
		rf.becomeFollower(rf.term, -1)
	}
}

func (rf *Raft) campaign() bool {
	rf.becomeCandidate()
	rf.votes[rf.me] = 1
	lastLogIndex := rf.raftLog.GetLastIndex()
	lastLogTerm := rf.raftLog.GetLastTerm()
	rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx != rf.me {
			var msg RequestVoteArgs
			msg.From = rf.me
			msg.Term = rf.term
			msg.LastLogIndex = lastLogIndex
			msg.LastLogTerm = lastLogTerm
			msg.To = idx
			go rf.sendRequestVote(msg)
			//rf.argsChan <- msg
		}
	}
	return false
}

func (rf *Raft) send() {
	for {
		select {
		case msg := <-rf.msgChan:
			{
				if msg.MsgType == MsgAppend {
					rf.sendAppendEntries(msg)
				} else if msg.MsgType == MsgHeartbeat {
					rf.sendHeartbeat(msg)
				}
			}
		}
	}
}

func (rf *Raft) step() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			if rf.lastHeartBeat > 200 {
				rf.lastHeartBeat = 0
				rf.leaderBroadCast() // unlock in broad heartbeat
				//continue
			} else if rf.lastElection > 1000 {
				rf.lastElection = 0
				rf.maybeLose()
			}
		} else if rf.lastElection >= rf.rdElectionTimeout {
			fmt.Printf("%d (state: %d)campaign begin at term:%d, at %d.\n", rf.me, rf.state, rf.term, rf.ts)
			rf.lastElection = 0
			rf.campaign()
			continue
		}
		rf.lastHeartBeat += 100
		rf.lastElection += 50
		rf.ts += 50
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
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
	rf.term = 0
	rf.vote = -1
	rf.rdElectionTimeout = int32(1000 + rand.Intn(5) * 100)
	for  {
		if _, ok := electionTimes[rf.rdElectionTimeout]; ok {
			rf.rdElectionTimeout += 100
			//rf.rdElectionTimeout = int32(500 + rand.Intn(5) * 100)
		} else {
			break
		}
	}
	electionTimes[rf.rdElectionTimeout] = true
	rf.lastHeartBeat = 0
	rf.lastElection = 0
	rf.ts = 0
	rf.applySM = applyCh
	//rf.appendChan = make(chan AppendReply, 1000)
	//rf.voteChan = make(chan RequestVoteReply, 1000)
	rf.msgChan = make(chan AppendMessage, 10000)
	rf.argsChan = make(chan RequestVoteArgs, 10000)
	rf.votes = make([]int, len(rf.peers))
	rf.leader = -1
	rf.actives = make([]bool, len(rf.peers))
	// Your initialization code here.
	rf.becomeFollower(0, -1)
	rf.readPersist(persister.ReadRaftState())
	go rf.step()
	go rf.send()
	fmt.Printf("%d : random election timeout: %d\n", rf.me, rf.rdElectionTimeout)
	return rf
}
