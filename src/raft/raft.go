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
	"sync/atomic"
	"sort"
	"fmt"
	"labgob"
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
type AppendMessage struct {
	Term			int
	From			int32
	To 				int
	Commited		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]Entry
}

type AppendResponse struct {
	Term			int
	Success			bool
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
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

type MsgType int
const (
	_ MsgType = iota
	MsgStop
	MsgPropose
	MsgHeartbeat
	MsgCampaign
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
	vote	  int32
	leader    int32
	state	  RoleState
	rdElectionTimeout int32
	lastHeartBeat int32
	lastElection int32
	applySM    chan ApplyMsg
	propose	   chan MsgType
	appendChan chan AppendResponse
	applyChan chan RequestVoteReply
	stop 	   chan bool
	raftLog	  UnstableLog
	nextIndex []int
	matchIndex []int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	isLeader := false
	if atomic.LoadInt32(&rf.leader) == int32(rf.me) {
		isLeader = true
		fmt.Printf("%d is Leader\n", rf.me)
	}
    return rf.term, isLeader
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
	//d.Decode(rf.me)
	//d.Decode(rf.term)
	//d.Decode(rf.vote)
	//d.Decode(rf.leader)
	//d.Decode(rf.state)
	//d.Decode(rf.rdElectionTimeout)
	//d.Decode(rf.lastHeartBeat)
	//d.Decode(rf.nextIndex)
	//d.Decode(rf.matchIndex)
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	From			int32
	To 				int
	LastLogIndex 	int
	LastLogTerm		int    
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("%d AccessRequest vote for: %d, Term: %d\n", rf.me, args.From, args.Term)
	defer fmt.Printf("%d Endrequest Vote End\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d AccessRequest begin process\n", rf.me)
	if ((rf.leader == -1 && rf.vote == -1) || rf.vote == args.From || rf.term < args.Term) && rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%d agree vote for: %d\n", rf.me, args.From)
		rf.vote = args.From
		// rf.Term = args.Term
		reply.VoteGranted = true
		return
	}

	fmt.Printf("%d(%d) reject vote for: %d(%d), has voted to %d\n", rf.me, rf.term, args.From, args.Term, rf.vote)
	reply.VoteGranted = false
	reply.Term = rf.term    
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	Index := len(rf.raftLog.Entries)
	e := Entry{bytesBuffer.Bytes(), rf.term, Index}
	idx := len(rf.raftLog.Entries)
	rf.raftLog.Entries = append(rf.raftLog.Entries, e)
	rf.mu.Unlock()
	rf.propose <- MsgPropose
	//accept := rf.sendAppendEntries()
	for t := 0; t < 20; t ++{
		time.Sleep(time.Millisecond * 100)
		if rf.raftLog.applied >= idx {
			return idx, rf.term, true
		}
	}
	return idx, rf.term, rf.state == Leader
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

func (rf *Raft) AppendEntries(args *AppendMessage, reply *AppendResponse)  {
	fmt.Printf("%d access append entries from %d\n", rf.me, args.From)
	defer fmt.Printf("%d: EndAppendEntries %v\n", rf.me, time.Now().UnixNano())
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (args.Term > rf.term) {
		atomic.StoreInt32(&rf.lastHeartBeat, 0)
		rf.BecomeFollower(args.Term, args.From)
	} else if (args.Term < rf.term) {
		reply.Success = false
		return
	}
	atomic.StoreInt32(&rf.lastHeartBeat, 0)
	//rf.lastHeartBeat = time.Now()
	// copy entry
	Index := len(rf.raftLog.Entries)
	if rf.raftLog.Entries[args.PrevLogIndex].Term == args.PrevLogTerm {
		lastIndex := Index
		for _, e := range args.Entries {
			e.Term = args.Term
			e.Index = Index
			Index += 1
			rf.raftLog.Entries = append(rf.raftLog.Entries, e)
			lastIndex = e.Index
		}
		reply.Success = true
/*		if rf.raftLog.applied < args.Commited {
			rf.persist()
		}*/
		for _, e := range rf.raftLog.Entries[rf.raftLog.applied + 1 : args.Commited + 1] {
			rf.applySM <- rf.createApplyMsg(e)
		}
		rf.raftLog.commited = MinInt(args.Commited, lastIndex)
		rf.raftLog.applied = args.Commited
		reply.Term = rf.term
	} else {
		reply.Success = false
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

func (rf *Raft) BecomeFollower(term int, leader int32) {
	if leader == int32(rf.me) {
		fmt.Printf("forbid[%d] become follower of self.\n", rf.me)
		panic(leader)
	}
	atomic.StoreInt32(&rf.leader, leader)
	fmt.Printf("%d become follower of %d in term: %d\n", rf.me, leader, term)
	rf.term = term
	rf.state = Follower
	for rf.propose != nil {
		rf.propose <- MsgStop
	}
	atomic.StoreInt32(&rf.leader, leader)
}

func (rf *Raft) BecomeLeader() {
	atomic.StoreInt32(&rf.lastHeartBeat, 0)
	fmt.Printf("%d become leader\n", rf.me)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int32, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.raftLog.Entries)
		rf.matchIndex[idx] = 0
	}
	//close(rf.propose)
	rf.state = Leader
	atomic.StoreInt32(&rf.leader, int32(rf.me))
	atomic.StoreInt32(&rf.lastHeartBeat, 0)
}

func (rf *Raft) BecomeCandidate() {
	rf.leader = -1
	rf.state = Candidate
	rf.vote = -1
}

func (rf *Raft) getUnsendEntries(idx int) ([]Entry, int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Entries := rf.raftLog.Entries[rf.nextIndex[idx]:]
	match := len(rf.raftLog.Entries) - 1
	return Entries, int32(match)
}

func (rf *Raft) createMessage() AppendMessage {
	var msg AppendMessage
	msg.Term = rf.term
	msg.From = int32(rf.me)
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

func (rf *Raft) sendAppendEntries() bool {
	fmt.Printf("%d: BeginSend append entries: %v\n", rf.me, time.Now().UnixNano())
	defer fmt.Printf("%d: EndSend append entries: %v\n", rf.me, time.Now().UnixNano())
	var failed int32 = 0
	var accept int32 = 0
	rf.mu.Lock()
	logNum := len(rf.raftLog.Entries)
	rf.mu.Unlock()
	for id, peer := range rf.peers {
		if id != int(rf.me) && logNum >= rf.nextIndex[id] {
			go func(idx int) {
				msg := rf.createMessage()
				var match int32
				msg.Entries, match = rf.getUnsendEntries(idx)
				for i := 0; i < 5; i ++{
					var reply AppendResponse
					ok := peer.Call("Raft.AppendEntries", &msg, &reply)
					if !ok {
						atomic.AddInt32(&failed, 1)
						break
					}
					if !reply.Success {
						rf.nextIndex[idx] --
						msg.Entries, match = rf.getUnsendEntries(idx)
					} else {
						rf.nextIndex[idx] = int(match) + 1
						atomic.StoreInt32(&rf.matchIndex[idx], match)
						break
					}
				}
				atomic.AddInt32(&accept, 1)
			}(id)
		}
	}
	quorum := len(rf.peers) / 2 + 1
	limit := len(rf.peers) - quorum
	peerCount := int32(len(rf.peers) - 1)
	for atomic.LoadInt32(&accept) < peerCount{
		if atomic.LoadInt32(&failed) > int32(limit) {
			return false
		}
		commits := make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			commits[i] = int(atomic.LoadInt32(&rf.matchIndex[i]))
		}
		sort.Ints(commits)
		rf.mu.Lock()
		if rf.raftLog.commited < commits[quorum] {
			rf.raftLog.commited = commits[quorum]
			for _, e := range rf.raftLog.GetUnApplyEntry() {
				rf.applySM <- rf.createApplyMsg(e)
			}
		}
		rf.mu.Unlock()
	}
	return true
}

func (rf *Raft) LeaderBroadCast() {
	fmt.Printf("%d: HeartBeat! \n", rf.me)
	defer fmt.Printf("%d: EndHeartBeat\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var reply AppendResponse
	accept := 1
	for idx, peer := range rf.peers {
		if idx != rf.me {
			msg := rf.createMessage()
			ok := peer.Call("Raft.AppendEntries", &msg, &reply)
			if ok {
				accept += 1
			} else {
				fmt.Printf("%d: connect %d failed\n", rf.me, idx)
			}
		}
	}
	quorum := len(rf.peers) / 2 + 1
	fmt.Printf("broad cast succes number: %d\n", accept)
	if accept < quorum {
		rf.BecomeFollower(rf.term, -1)
	}
}

func (rf *Raft) Campaign() bool {
	atomic.StoreInt32(&rf.lastHeartBeat, 0)
	fmt.Printf("%d campaign begin.\n", rf.me)
	defer fmt.Printf("%d campaign end. leader :%d, state: %d\n", rf.me, rf.leader, rf.state)
	rf.mu.Lock()
	rf.BecomeCandidate()
	//startCampaign := rf.lastHeartBeat
	rf.term += 1
	rf.vote = int32(rf.me)
	Term := rf.term
	LastLogIndex := rf.raftLog.GetLastIndex()
	LastLogTerm := rf.raftLog.GetLastTerm()
	rf.mu.Unlock()
	fmt.Printf("%d: VoteBegin. \n", rf.me)
	accept := 0
	for idx, _ := range rf.peers {
		if idx != rf.me {
			var msg RequestVoteArgs
			var resp RequestVoteReply
			msg.From = int32(rf.me)
			msg.Term = Term
			msg.LastLogIndex = LastLogIndex
			msg.LastLogTerm = LastLogTerm
			msg.To = idx
			ok := rf.sendRequestVote(msg.To, &msg, &resp)
			if !ok {
				time.Sleep(20 * time.Millisecond)
				fmt.Printf("%d: connect %d failed\n", rf.me, idx)
			} else {
				if resp.VoteGranted {
					accept += 1
				}
			}
		}
	}
	fmt.Printf("%d: VoteEnd. \n", rf.me)
	rf.mu.Lock()
	quorum := len(rf.peers) / 2 + 1
	if int(rf.vote) == rf.me {
		accept += 1
	}
	fmt.Printf("%d: vote end. accpet: %d, %d\n", rf.me, accept, quorum)
	if accept >= quorum && rf.state != Follower{
		// become leader
		rf.BecomeLeader()
		rf.mu.Unlock()
		rf.propose <- MsgHeartbeat
		return true
	}
	rf.mu.Unlock()
	//fmt.Printf("%d: Leader %d, state: %d\n", rf.me, rf.leader, rf.state)
	return false
}

const MilliSecond int64 = 1000000
func (rf *Raft) tickHeartBeat() {
	for {
		if rf.state == Leader {
			//rf.LeaderBroadCast()
			if atomic.AddInt32(&rf.lastHeartBeat, 50) > 150 {
				rf.propose <- MsgHeartbeat
			} else if rf.state == Follower || rf.state == Candidate {
				//atomic.AddInt32(&rf.lastHeartBeat, 10)
				if atomic.AddInt32(&rf.lastElection, 50) > rf.rdElectionTimeout {
					rf.propose <- MsgCampaign
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) step() {
	for {
		select {
		case x := <-rf.propose: {
			if x == MsgPropose {
				rf.sendAppendEntries()
			} else if x == MsgHeartbeat {
				fmt.Printf("receive %d: heartbeat\n", rf.me)
				rf.LeaderBroadCast()
			} else if x == MsgCampaign {
				rf.Campaign()
			}

		}

		case <-rf.stop:
			return
		case msg := <- rf.appendChan :{

		}
		}
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
	rf.term = 0
	rf.vote = -1
	rf.rdElectionTimeout = int32(600 + rand.Intn(5) * 50)
	for  {
		if _, ok := electionTimes[rf.rdElectionTimeout]; ok {
			rf.rdElectionTimeout += 50
			//rf.rdElectionTimeout = int32(500 + rand.Intn(5) * 100)
		} else {
			break
		}
	}
	electionTimes[rf.rdElectionTimeout] = true
	//rf.heartbeat = 80

	rf.lastHeartBeat = 0
	rf.lastElection = 0
	rf.propose = make(chan MsgType, 100)
	rf.stop = make(chan bool, 100)
	rf.leader = -1
	// Your initialization code here.
	rf.BecomeFollower(0, -1)
	rf.readPersist(persister.ReadRaftState())
	go rf.tickHeartBeat()
	go rf.step()
	fmt.Printf("%d : random election timeout: %d\n", rf.me, rf.rdElectionTimeout)
	return rf
}
