package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	OpType  string
	Key     string
	Value	string
	Idx     uint64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type StatisticInfo struct {
	createSnapshotTimes int
	applySnapshotTimes int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	stopChan  chan bool

	maxraftstate int // snapshot if log grows this big
	dataIndex   int32
	pendingSnapshot int32
	logIndex    int
	cond    map[int]chan int
	storage  Storage
	stop     bool
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.OpType = "Get"
	op.Idx = 0
	op.Key = args.Key
	index, term, ret := kv.rf.Start(op);
	//DPrintf("Get a key %s in index: %d\n", args.Key, index)
	if !ret {
		reply.WrongLeader = true
		return
	}
	DPrintf("%d Begin Get operation, key: %s, idx: %d, index: %d, term: %d\n",
		kv.me, args.Key, args.Idx, index, term)

	c := kv.CreateChannel(index)
	defer kv.CloseChannel(index)

	for ; kv.rf.IsLeader(); {
		select {
			case applyTerm :=  <- c: {
				if applyTerm > term {
					reply.WrongLeader = true
					reply.Err = OK
					DPrintf("%d Failed to get key: %s, idx: %d, index: %d, term: %d, apply term: %d\n",
						kv.me, args.Key, args.Idx, index, term, applyTerm)
					return
				} else {
					reply.Value = kv.storage.Get(args.Key)
					DPrintf("End Get operation, key: %s, idx: %d, index: %d, value: %s\n",
						args.Key, args.Idx, index, reply.Value)
					reply.WrongLeader = false
					reply.Err = OK
					return
				}
			}
			case <-time.After(time.Duration(50) * time.Millisecond): {
				kv.mu.Lock()
				if kv.stop {
					reply.WrongLeader = true
					reply.Err = OK
					kv.mu.Unlock()
					return
				}
				kv.mu.Unlock()
				break
			}
		}
		DPrintf("%d Get apply %s index: %d,  to apply : %d\n", kv.me, args.Key, kv.dataIndex, index)
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.storage.CheckCommand(args.Idx) {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("_______________replicate command key: %s, value: %s\n", args.Key, args.Value)
		return
	}
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		//reply.Err = ErrNoKey
		//DPrintf("%d is not leader, leader is %d\n", kv.me, kv.rf.GetLeader())
	} else {
		kv.appendValue(args, reply)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	DPrintf("Kill Raft %d begin\n", kv.me)
	kv.rf.Kill()
	kv.mu.Lock()
	kv.stop = true
	kv.mu.Unlock()
	DPrintf("Kill KV %d begin\n", kv.me)
	kv.stopChan <- true
	DPrintf("Kill KV %d\n", kv.me)
	for !atomic.CompareAndSwapInt32(&kv.pendingSnapshot, 0, 1) {
		DPrintf("kill while there is a pending snapshot\n")
		time.Sleep(time.Second)
	}
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.storage.kv = make(map[string]string)
	kv.storage.commands = make(map[uint64]time.Time)
	kv.cond = make(map[int]chan int)
	kv.stopChan = make(chan bool)
	kv.stop = false
	kv.persister = persister
	kv.pendingSnapshot = 0
	DPrintf("Start KV %d\n", kv.me)
	go kv.startApplyMsgThread()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applySnapshot(s *raft.Snapshot) {
	start := time.Now()
	DPrintf("===begin %d Apply a snapshot in index: %d\n", kv.me, s.Index)
	kv.mu.Lock()
	if e := kv.storage.ApplySnapshot(s); e != nil {
		DPrintf("%d apply error: %s\n", kv.me, e.Error())
		panic("Apply Error")
	}
	kv.dataIndex = int32(s.DataIndex)
	kv.logIndex = s.Index
	DPrintf("===end %d Apply a snapshot in index: %d\n", kv.me, s.Index)
	kv.mu.Unlock()
	raft.CalcRuntime(start, "ApplySnapshot")
}

func (kv *KVServer) applyMsgFromRaft(term int, index int32, op* Op) {
	if kv.dataIndex + 1 != index {
		DPrintf("Error for msg: commandIndex(%d) != kv.index(%d) + 1\n", index, kv.dataIndex)
	}
	if op.OpType == "Get" {
		DPrintf("%d Apply a msg of key: %s in index: %d, term: %d, type: %s\n",
			kv.me, op.Key, index, term, op.OpType)
	} else if op.OpType == "Append" {
		value := kv.storage.Get(op.Key)
		value += op.Value
		kv.storage.Put(op.Idx, op.Key, value)
		DPrintf("%d Apply a msg of key: %s in index: %d, value: %s, type: %s\n",
			kv.me, op.Key, index, value, op.OpType)
	} else {
		kv.storage.Put(op.Idx, op.Key, op.Value)
		DPrintf("%d Apply a msg of key: %s in index: %d, value: %s, type: %s\n",
			kv.me, op.Key, index, op.Value, op.OpType)
	}
	atomic.StoreInt32(&kv.dataIndex, index)
	kv.Notify(int(index), term)
}

func (kv *KVServer) createSnapshot(data []byte, index int) {
	if !atomic.CompareAndSwapInt32(&kv.pendingSnapshot, 0, 1) {
		return
	}
	if kv.rf != nil {
		kv.rf.CreateSnapshot(data, index, kv.maxraftstate)
	}
	for !atomic.CompareAndSwapInt32(&kv.pendingSnapshot, 1, 0) {
		DPrintf("===============ATOMIC SET ERROR============\n")
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (kv *KVServer) startApplyMsgThread() {
	for {
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate &&
			atomic.LoadInt32(&kv.pendingSnapshot) == 0 {
			DPrintf("=========================%d: raft size: %d, maxraftsize: %d, kv[0]=%s\n",
				kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, kv.storage.Get("0"))
			go kv.createSnapshot(kv.storage.Bytes(), kv.logIndex)
		}
		select {
		case msg := <-kv.applyCh : {
			if msg.Snap != nil {
				kv.applySnapshot(msg.Snap)
				break
			} else if !msg.CommandValid {
				break
			}
			data := msg.Command.(Op)
			kv.applyMsgFromRaft(msg.Term, int32(msg.CommandIndex), &data)
			kv.logIndex = msg.LogIndex
		}
		case <- kv.stopChan : {
			return
		}
		case <-time.After(time.Duration(100) * time.Millisecond) : {
			break
		}
		}
	}
	// Your code here, if desired.
}

func (kv *KVServer) appendValue(args *PutAppendArgs, reply *PutAppendReply) bool {
	var op Op
	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Idx = args.Idx
	index, term, ret := kv.rf.Start(op);
	if !ret {
		reply.WrongLeader = true
		reply.Err = OK
		DPrintf("Put a key %s to error server. leader is %d\n", args.Key, kv.rf.GetLeader())
		return false
	}
	c := kv.CreateChannel(index)
	defer kv.CloseChannel(index)
	DPrintf("%d Begin %s a value %s to key %s in index: %d, term: %d\n", kv.me,
		args.Op, args.Value, args.Key, index, term)
	for ; kv.rf.IsLeader(); {
		select {
			case applyTerm :=  <- c: {
				if applyTerm > term {
					reply.WrongLeader = true
					reply.Err = OK
					DPrintf("%d is not leader. Stop Wait\n", kv.me)
					return false
				} else {
					reply.WrongLeader = false
					reply.Err = OK
					return true
				}
			}
			case <-time.After(time.Duration(100) * time.Millisecond): {
				kv.mu.Lock()
				if kv.stop {
					kv.mu.Unlock()
					reply.WrongLeader = true
					reply.Err = OK
					return false
				}
				kv.mu.Unlock()
				break
			}
		}
		//kv.rf.DebugLog()
		//DPrintf("%d PutAppend apply data index: %d,  to apply : %d\n", kv.me, kv.dataIndex, index)
	}
	return true
}

func (s *KVServer) CreateChannel(idx int) chan int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.cond[idx]; ok {
		return c
	}
	c := make(chan int, 1)
	s.cond[idx] = c
	return c
}

func (s *KVServer) Notify(idx int, term int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.cond[idx]; ok {
		c <- term
	} else {
		c := make(chan int, 1)
		s.cond[idx] = c
		c <- term
		if s.rf.IsLeader() {
			DPrintf("%d missing notify channel: (%d, %d)", s.me, idx, term)
		}
	}
}

func (s *KVServer) CloseChannel(idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cond, idx)
}

