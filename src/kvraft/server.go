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
	Idx     int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	stop    int32
	dataIndex   int
	storage  Storage
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	fmt.Printf("Receive a Get operation, key: %s, idx: %d\n", args.Key, args.Idx)
	// Your code here.
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.WrongLeader = false
		reply.Err = Err(WriteLeader(kv.rf.GetLeader()))
	} else {
		if args.Op == "Append" {
			kv.appendValue(args, reply)
		} else {
			kv.putValue(args, reply)
		}
	}
	fmt.Printf("Receive a %v operation, key: %s, value: %s, Idx: %d\n", args.Op, args.Key, args.Value, args.Idx)
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
	kv.applyCh <- msg
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.startApplyMsgThread()
	// You may need initialization code here.

	return kv
}


func (kv *KVServer) applyMsgFromRaft(msg *raft.ApplyMsg, op* Op) {
	if kv.dataIndex + 1 != msg.CommandIndex {
		fmt.Printf("Error for msg: commandIndex(%d) != kv.index(%d) + 1\n", msg.CommandIndex, kv.dataIndex)
	}
	if op.OpType == "Append" {
		value, idx := kv.storage.Get(op.Key)
		if idx == op.Idx {
			return
		}
		value += op.Value
		kv.storage.Put(op.Key, value, op.Idx)
	} else {
		kv.storage.Put(op.Key, op.Value, op.Idx)
	}
	kv.dataIndex = msg.CommandIndex
}

func (kv *KVServer) startApplyMsgThread() {
	for atomic.LoadInt32(&kv.stop) == 0 {
		select {
		case msg := <-kv.applyCh : {
			data := msg.Command.(Op)
			if data.OpType == "stop" {
				break
			}
			kv.applyMsgFromRaft(&msg, &data)
		}
		}
	}
	atomic.StoreInt32(&kv.stop, 2)
	// Your code here, if desired.
}


func (kv *KVServer) appendValue(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.Idx = args.Idx
	op.OpType = args.Op

	index, _, ret := kv.rf.Start(op);
	if !ret {
		reply.WrongLeader = true
		reply.Err = Err(WriteLeader(kv.rf.GetLeader()))
		return
	}
	for ; kv.rf.IsLeader(); {
		if kv.dataIndex >= index {
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
	reply.WrongLeader = true
	reply.Err = Err(WriteLeader(kv.rf.GetLeader()))
}

func (kv *KVServer) putValue(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.Idx = args.Idx
	op.OpType = args.Op
	index, _, ret := kv.rf.Start(op);
	if !ret {
		reply.WrongLeader = true
		reply.Err = Err(WriteLeader(kv.rf.GetLeader()))
		return
	}
	for ; kv.rf.IsLeader(); {
		if kv.dataIndex >= index {
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(40) * time.Millisecond)
	}
	reply.WrongLeader = true
	reply.Err = Err(WriteLeader(kv.rf.GetLeader()))
}
