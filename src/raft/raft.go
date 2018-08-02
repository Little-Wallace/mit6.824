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
	"encoding/binary"
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
)

type AppendMessage struct {
	MsgType			MessageType
	Term			int
	From			int32
	To 				int
	Commited		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]Entry
	Success			bool
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

type Entry struct {
	Data []byte
	Term int
	Index int
}

type UnstableLog struct {
	Entries		[]Entry
	commited	int
	applied		int
}

func (log *UnstableLog) GetLastIndex() int {
	return log.Entries[len(log.Entries) - 1].Index
}

func (log *UnstableLog) GetLastTerm() int {
	return log.Entries[len(log.Entries) - 1].Term
}

func (log *UnstableLog) IsUpToDate(Index int, Term int) bool {
	return Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex())
}

func (log *UnstableLog) GetUnApplyEntry() []Entry {
	return log.Entries[log.applied + 1 : log.commited + 1]
}

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
	leader    int32
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
	rf.lastElection = 0
	if rf.term > args.Term {
		fmt.Printf("%d %d reject smaller term: %d\n", rf.me, rf.term, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}
	reply.Term = args.Term
	if ((rf.leader == -1 && rf.vote == -1) || rf.vote == args.From || rf.term < args.Term) &&
		rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%d agree vote for: %d\n", rf.me, args.From)
		rf.vote = args.From
		reply.VoteGranted = true
		rf.becomeFollower(args.Term, -1)
		return
	}
	fmt.Printf("%d(%d) reject vote for: %d(%d), has voted to %d\n", rf.me, rf.term, args.From, args.Term, rf.vote)
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendMessage, reply *AppendReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d(%d) access append entries from %d(%d)\n", rf.me, rf.term, args.From, args.Term)
	if !rf.handleHeartbeat(args.From, args.Term, args.MsgType) {
		reply.Success = false
		reply.To = rf.me
		return
	}

	defer fmt.Printf("%d: EndAppendEntries \n", rf.me, )

	index := len(rf.raftLog.Entries)
	if args.PrevLogIndex >= index {
		fmt.Printf("%d(index: %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, args.From, args.PrevLogIndex)
		reply.Success = false
		reply.Commited = index - 1
		//reply.Commited =
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
		}
		reply.Success = true
		rf.raftLog.commited = MinInt(args.Commited, lastIndex)
		if rf.raftLog.applied < rf.raftLog.commited && rf.raftLog.commited < len(rf.raftLog.Entries) {
			for _, e := range rf.raftLog.Entries[rf.raftLog.applied + 1 : rf.raftLog.commited + 1] {
				//rf.applySM <- rf.createApplyMsg(e)
				fmt.Printf("%d Apply entre : term: %d, index: %d\n", rf.me, e.Term, e.Index)
			}
		}
		rf.raftLog.applied = rf.raftLog.commited
		reply.Term = rf.term
		reply.Commited = lastIndex
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
	if !rf.handleHeartbeat(msg.From, msg.To, msg.MsgType) {
		reply.Success = false
		reply.To = rf.me
		reply.Term = rf.term
		return
	}
	reply.Success = true
	reply.To = rf.me
	reply.Term = msg.Term
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
func (rf *Raft) sendRequestVote(args* RequestVoteArgs) bool {
	var reply RequestVoteReply
	ts := rf.ts
	fmt.Printf("send request vote from %d to %d  at %d\n", args.From, args.To, ts)
	ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply)
	reply.To = args.To
	if ok {
		fmt.Printf("send request vote from %d to %d ok\n", args.From, args.To)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		fmt.Printf("send request vote from %d to %d get lock\n", args.From, args.To)
		ts = rf.ts
		rf.handleVoteReply(&reply)
	}
	fmt.Printf("send request vote from %d to %d at %d, result: %t\n", args.From, args.To, ts, ok)
	return ok
	//ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) handleVoteReply(reply* RequestVoteReply) {
	fmt.Printf("%d(%d): receive vote reply from %d(%d), accept: %t at %d\n",
		rf.me, rf.term, reply.To, reply.Term, reply.VoteGranted, rf.ts)
	if reply.Term > rf.term {
		rf.becomeFollower(rf.term, -1)
	}
	if reply.VoteGranted && rf.state == Candidate {
		rf.votes[reply.To] = 1
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
			rf.leaderBroadCast()
		} else if reject == len(rf.peers) - quorum {
			rf.becomeFollower(rf.term, -1)
		}
	} else {
		rf.votes[reply.To] = 0
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
	if rf.leader != int32(rf.me) {
		return len(rf.raftLog.Entries), rf.term, false
	}
	bytesBuffer := bytes.NewBuffer([]byte{})
	rf.mu.Lock()
	binary.Write(bytesBuffer, binary.BigEndian, command)
	index := len(rf.raftLog.Entries)
	e := Entry{bytesBuffer.Bytes(), rf.term, index}
	var msg AppendMessage
	msg.MsgType = MsgPropose
	msg.Entries = append(msg.Entries, e)
	rf.mu.Unlock()
	rf.msgChan <- msg
	//accept := rf.sendAppendEntries()
	for t := 0; t < 20; t ++{
		time.Sleep(time.Millisecond * 100)
		if rf.raftLog.applied >= index {
			return index, rf.term, true
		}
	}
	return index, rf.term, rf.state == Leader
}

func (rf *Raft) createApplyMsg(e Entry) ApplyMsg {
	var applyMsg ApplyMsg
	var tmp int
	bytesBuffer := bytes.NewBuffer(e.Data)
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	applyMsg.CommandIndex = e.Index
	applyMsg.Command = tmp
	return applyMsg
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) sendAppendEntries(msg *AppendMessage) bool {
	var reply AppendReply
	ts := rf.ts
	fmt.Printf("send append msg from %d to %d, at %d\n", msg.From, msg.To, ts)
	ok := rf.peers[msg.To].Call("Raft.AppendEntries", msg, &reply)
	reply.To = msg.To
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		ts = rf.ts
		rf.handleAppendReply(msg, &reply)
	}
	fmt.Printf("AppendEntries from %d to %d, %t, at %d\n", msg.From, msg.To, ok, ts)
	return ok
}

func (rf *Raft) handleAppendReply(msg* AppendMessage, reply* AppendReply) {
	fmt.Printf("%d handleAppendReply from %d at %d\n", rf.me, msg.To, rf.ts)
	if !rf.handleHeartbeat(int32(msg.To), reply.Term, MsgAppendReply) {
		return
	}
	rf.actives[msg.To] = true
	if !reply.Success {
		fmt.Printf("%d(%d) handleAppendReply failed, from %d(%d)\n", rf.me, rf.term, msg.To, msg.Term)
		rf.nextIndex[reply.To] --
		msg := rf.createMessage(reply.To)
		msg.Entries, _ = rf.getUnsendEntries(rf.nextIndex[reply.To])
		rf.msgChan <- msg
		fmt.Printf("%d send again handleAppendReply\n", rf.me)
	} else {
		fmt.Printf("%d send handleAppendReply success\n", rf.me)
		rf.nextIndex[reply.To] = reply.Commited + 1
		rf.matchIndex[reply.To] = reply.Commited
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
		fmt.Printf("%d commit %d, to commit %d, apply %d, all: %d\n",
			rf.me, rf.raftLog.commited, commits[quorum], rf.raftLog.applied, len(rf.raftLog.Entries))
		if rf.raftLog.commited < commits[quorum] {
			rf.raftLog.commited = commits[quorum]
			for _, e := range rf.raftLog.GetUnApplyEntry() {
				//rf.applySM <- rf.createApplyMsg(e)
				fmt.Printf("leader: %d Apply entre : term: %d, index: %d\n", rf.me, e.Term, e.Index)
				rf.raftLog.applied += 1
			}
			fmt.Printf("%d apply message\n", rf.me)
		}
		fmt.Printf("%d send handleAppendReply end\n", rf.me)
	}
}

func (rf *Raft) handleHeartbeat(from int32, term int, msgType MessageType) bool {
	if term > rf.term {
		rf.becomeFollower(term, from)
	} else if (term < rf.term) {
		return false
	} else if rf.state == Candidate || (rf.state == Follower && rf.leader == -1) {
		if msgType == MsgAppendReply{
			rf.becomeFollower(term, from)
		} else {
			rf.becomeFollower(term, -1)
		}
	}
	// copy entry
	if rf.state != Leader {
		rf.lastElection = 0
	}
	return true
}

func (rf *Raft) sendHeartbeat(msg *AppendMessage) {
	var reply AppendReply
	ts := rf.ts
	reply.To = msg.To
	fmt.Printf("send append msg from %d to %d, at %d\n", msg.From, msg.To, ts)
	ok := rf.peers[msg.To].Call("Raft.HeartBeat", msg, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		ts = rf.ts
		if rf.handleHeartbeat(int32(msg.To), reply.Term, MsgAppendReply) {
			rf.actives[msg.To] = true
		}
	}
	fmt.Printf("AppendEntries from %d to %d, %t, at %d\n", msg.From, msg.To, ok, ts)
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

func (rf *Raft) becomeFollower(term int, leader int32) {
	if rf.term != term {
		rf.vote = int(leader)
	}
	rf.reset(term)
	rf.state = Follower
	rf.leader = leader
	fmt.Printf("%d become follower of %d in term: %d when %d\n", rf.me, leader, term, rf.ts)
}

func (rf *Raft) becomeLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.raftLog.Entries)
		rf.matchIndex[idx] = len(rf.raftLog.Entries) - 1
	}
	index := len(rf.raftLog.Entries)
	rf.raftLog.Entries = append(rf.raftLog.Entries, Entry{[]byte{}, rf.term, index})
	rf.state = Leader
	rf.leader = int32(rf.me)
	rf.lastHeartBeat = 0
	rf.lastElection = 0
	fmt.Printf("%d become leader when %d\n", rf.me, rf.ts)
}

func (rf *Raft) becomeCandidate() {
	rf.leader = -1
	rf.state = Candidate
	rf.reset(rf.term + 1)
	rf.vote = rf.me
	rf.argsChan = make(chan RequestVoteArgs, 10000)
	fmt.Printf("%d become candidate when %d\n", rf.me, rf.ts)
}

func (rf *Raft) getUnsendEntries(idx int) ([]Entry, int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Entries := rf.raftLog.Entries[rf.nextIndex[idx]:]
	match := len(rf.raftLog.Entries) - 1
	return Entries, int32(match)
}

func (rf *Raft) createMessage(to int) AppendMessage {
	var msg AppendMessage
	msg.Term = rf.term
	msg.From = int32(rf.me)
	msg.To = to
	msg.Commited = rf.raftLog.commited
	msg.PrevLogIndex = rf.raftLog.GetLastIndex()
	msg.PrevLogTerm = rf.raftLog.GetLastTerm()
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


func (rf *Raft) propose(msg *AppendMessage) {
	fmt.Printf("%d: BeginSend append entries: %v\n", rf.me, time.Now().UnixNano())
	defer fmt.Printf("%d: EndSend append entries: %v\n", rf.me, time.Now().UnixNano())
	logNum := len(rf.raftLog.Entries)
	rf.raftLog.Entries = append(rf.raftLog.Entries, msg.Entries[0])
	for id, _ := range rf.peers {
		if id != int(rf.me) && logNum >= rf.nextIndex[id] {
			msg := rf.createMessage(id)
			msg.MsgType = MsgAppend
			msg.Entries, _ = rf.getUnsendEntries(id)
			rf.msgChan <- msg
		}
	}
}

func (rf *Raft) leaderBroadCast() {
	fmt.Printf("%d: HeartBeat! %d\n", rf.me, rf.ts)
	defer fmt.Printf("%d: EndHeartBeat! %d\n", rf.me, rf.ts)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			msg := rf.createMessage(idx)
			msg.MsgType = MsgHeartbeat
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
	for idx, _ := range rf.peers {
		if idx != rf.me {
			var msg RequestVoteArgs
			msg.From = rf.me
			msg.Term = rf.term
			msg.LastLogIndex = lastLogIndex
			msg.LastLogTerm = lastLogTerm
			msg.To = idx
			rf.argsChan <- msg
		}
	}
	return false
}

func (rf *Raft) send() {
/*	ms := make([]AppendMessage, 100)
	vs := make([]RequestVoteArgs, 100)
	mi := 0
	vi := 0*/
	for {
		select {
		case args := <- rf.argsChan :{
			rf.sendRequestVote(&args)
		}
		case msg := <-rf.msgChan: {
			if msg.MsgType == MsgPropose {
				rf.mu.Lock()
				if rf.state == Leader {
					rf.propose(&msg)
				}
				rf.mu.Unlock()
			} else if msg.MsgType == MsgAppend {
				rf.sendAppendEntries(&msg)
			} else if msg.MsgType == MsgHeartbeat{
				rf.sendHeartbeat(&msg)
			} else if msg.MsgType == MsgStop {
				return
			}
		}
		default:
			time.Sleep(20 * time.Millisecond)
			break
		}
	}
}

func (rf *Raft) step() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			if rf.lastHeartBeat > 100 {
				rf.leaderBroadCast()
				rf.lastHeartBeat = 0
			}
			if rf.lastElection > 500 {
				rf.lastElection = 0
				rf.maybeLose()
			}
		} else {
			if rf.lastElection >= rf.rdElectionTimeout {
				fmt.Printf("%d campaign begin at term:%d, ts: %d.\n", rf.me, rf.lastElection, rf.ts)
				rf.lastElection = 0
				rf.campaign()
			}
		}
		rf.lastHeartBeat += 40
		rf.lastElection += 40
		rf.ts += 40
		rf.mu.Unlock()
		time.Sleep(time.Duration(40) * time.Millisecond)
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
	rf.raftLog.Entries = append(rf.raftLog.Entries, Entry{[]byte{}, 0, 0})
	rf.raftLog.commited = 0
	rf.raftLog.applied = 0
	rf.term = 0
	rf.vote = -1
	rf.rdElectionTimeout = int32(600 + rand.Intn(5) * 60)
	for  {
		if _, ok := electionTimes[rf.rdElectionTimeout]; ok {
			rf.rdElectionTimeout += 100
			//rf.rdElectionTimeout = int32(500 + rand.Intn(5) * 100)
		} else {
			break
		}
	}
	electionTimes[rf.rdElectionTimeout] = true
	//rf.heartbeat = 80

	rf.lastHeartBeat = 0
	rf.lastElection = 0
	rf.ts = 0
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
	//go rf.tickHeartBeat()
	go rf.step()
	go rf.send()
	fmt.Printf("%d : random election timeout: %d\n", rf.me, rf.rdElectionTimeout)
	return rf
}
