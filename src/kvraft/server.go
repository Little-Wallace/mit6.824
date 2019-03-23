package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	stop    int32
	dataIndex   int32
	storage  Storage
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.OpType = "Get"
	op.Idx = 0
	index, _, ret := kv.rf.Start(op);
	fmt.Printf("Get a key %s in index: %d\n", args.Key, index)
	if !ret {
		reply.WrongLeader = true
		reply.Err = Err(WriteLeader(kv.me, kv.rf.GetLeader()))
		return
	}
	fmt.Printf("Receive a Get operation, key: %s, idx: %d\n", args.Key, args.Idx)
	for ; kv.rf.IsLeader(); {
		if kv.dataIndex >= int32(index) {
			reply.WrongLeader = false
			reply.Value = kv.storage.Get(args.Key)
			reply.Err = ""
			fmt.Printf("finish Get operation, key: %s, value: %s\n", args.Key, reply.Value)
			return
		}
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
	reply.WrongLeader = true
	reply.Err = Err(WriteLeader(kv.me, kv.rf.GetLeader()))
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.storage.CheckCommand(args.Idx) {
		reply.WrongLeader = false
		reply.Err = ""
		fmt.Printf("_______________replicate command key: %s, value: %s\n", args.Key, args.Value)
		return
	}
	fmt.Printf("Begin PutAppend key to %d : key %s, value: %s, type: %s\n", kv.me, args.Key, args.Value, args.Op)
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		reply.Err = Err(WriteLeader(kv.me, kv.rf.GetLeader()))
		fmt.Printf("%d is not leader, leader is %d\n", kv.me, kv.rf.GetLeader())
	} else {
		kv.appendValue(args, reply)
	}
	fmt.Printf("Finish a %s operation, key: %s, value: %s, Idx: %d\n", args.Op, args.Key, args.Value, args.Idx)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.stop, 1)
	var msg raft.ApplyMsg
	var op Op
	op.OpType = "stop"
	msg.Command = op
	msg.CommandValid = true
	//kv.applyCh <- msg
	for atomic.LoadInt32(&kv.stop) != 2 {
		time.Sleep(time.Duration(10) * time.Millisecond)
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
	//kv.storage.kv = make(map[string][]byte)
	kv.storage.kv = make(map[string]string)
	kv.storage.commands = make(map[uint64]time.Time)
	kv.persister = persister
	go kv.startApplyMsgThread()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applySnapshot(s *raft.Snapshot) {
	kv.mu.Lock()
	if e := kv.storage.ApplySnapshot(s); e != nil {
		fmt.Printf("%d apply error: %s\n", kv.me, e.Error())
		panic("Apply Error")
	}
	kv.dataIndex = int32(s.DataIndex)
	fmt.Printf("%d Apply a snapshot in index: %d\n", kv.me, s.Index)
	kv.mu.Unlock()
}

func (kv *KVServer) applyMsgFromRaft(index int32,  op* Op) {
	if kv.dataIndex + 1 != index {
		fmt.Printf("Error for msg: commandIndex(%d) != kv.index(%d) + 1\n", index, kv.dataIndex)
	}
	if op.OpType == "Get" {
	} else if op.OpType == "Append" {
		value := kv.storage.Get(op.Key)
		value += op.Value
		kv.storage.Put(op.Idx, op.Key, value)
		fmt.Printf("%d Apply a msg of type: %s, key: %s in index: %d, value: %s\n",
			kv.me, op.OpType, op.Key, index, value)
	} else {
		kv.storage.Put(op.Idx, op.Key, op.Value)
		fmt.Printf("%d Apply a msg of type: %s, key: %s in index: %d, value: %s\n",
			kv.me, op.OpType, op.Key, index, op.Value)
	}
	atomic.StoreInt32(&kv.dataIndex, index)
}

func (kv *KVServer) startApplyMsgThread() {
	lastIndex := -1
	for atomic.LoadInt32(&kv.stop) == 0 {
		select {
		case msg := <-kv.applyCh : {
			if msg.Snap != nil {
				kv.applySnapshot(msg.Snap)
				break
			} else if !msg.CommandValid {
				break
			}
			data := msg.Command.(Op)
			if data.OpType == "stop" {
				break
			}
			kv.applyMsgFromRaft(int32(msg.CommandIndex), &data)
			lastIndex = msg.LogIndex
		}
		case <-time.After(time.Duration(10) * time.Millisecond) : {
			if kv.stop != 0 || kv.maxraftstate == -1 {
				break
			}
			if lastIndex != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				fmt.Printf("=========================%d: raft size: %d, maxraftsize: %d, kv[0]=%s\n",
					kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, kv.storage.Get("0"))
				if !kv.rf.CreateSnapshot(kv.storage.Bytes(), lastIndex) {
					fmt.Printf("Apply Error\n")
				}
			}
		}
		}
	}
	atomic.StoreInt32(&kv.stop, 2)
	// Your code here, if desired.
}


func (kv *KVServer) appendValue(args *PutAppendArgs, reply *PutAppendReply) bool {
	var op Op
	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Idx = args.Idx
	index, _, ret := kv.rf.Start(op);
	if !ret {
		reply.WrongLeader = true
		reply.Err = Err(WriteLeader(kv.me, kv.rf.GetLeader()))
		fmt.Printf("Put a key %s to error server. leader is %d\n", args.Key, kv.rf.GetLeader())
		return false
	}
	//kv.mu.Lock()
	//kv.commands[args.Idx] = time.Now()
	//kv.mu.Unlock()
	fmt.Printf("%s a value %s to key %s in index: %d\n", args.Op, args.Value, args.Key, index)
	for ; kv.rf.IsLeader(); {
		if atomic.LoadInt32(&kv.dataIndex) >= int32(index) {
			reply.WrongLeader = false
			reply.Err = ""
			return true
		}
		kv.rf.DebugLog()
		fmt.Printf("%d max apply data index: %d\n", kv.me, kv.dataIndex)
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
	reply.WrongLeader = true
	reply.Err = Err(WriteLeader(kv.me, kv.rf.GetLeader()))
	fmt.Printf("%d is not leader. Stop Wait\n", kv.me)
	return true
}

