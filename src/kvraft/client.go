package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
	"time"
	"sync"
	"log"
)

func CalcRuntime(t time.Time, f string) {
	now := time.Now()
	log.Printf("%s cost %f millisecond\n", f, now.Sub(t).Seconds() * 1000)
}


type Clerk struct {
	servers []*labrpc.ClientEnd
	idx     uint64
	name    int64
	leader  int
	addrs   []int
	mu      sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.idx = uint64(nrand() % 1000000 + 1) * 10000 + uint64(time.Now().Second() * time.Now().Minute())
	ck.leader = 0
	ck.addrs = make([]int, len(servers) + 1)
	for idx, _ := range ck.addrs {
		ck.addrs[idx] = -1
	}

	DPrintf("Make Clerk %d\n", ck.idx)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	start := time.Now()
	defer CalcRuntime(start, "Get Key")
	DPrintf("Begin Get key: %s\n", key)
	leader := ck.leader
	for ; ; {
		var reply GetReply
		//DPrintf("Get key %s from %d begin \n", key, leader)
		if !ck.servers[leader].Call("KVServer.Get", &args, &reply) {
			time.Sleep(time.Duration(20) * time.Millisecond)
			continue
		}
		if reply.WrongLeader {
			//lastLeader := leader
			leader = (leader + 1) % len(ck.servers)
			time.Sleep(time.Duration(20) * time.Millisecond)
			DPrintf("Get key %s failed,  change leader to %d\n", key, leader)
		} else if reply.Err == OK {
			ck.mu.Lock()
			ck.leader = leader
			ck.mu.Unlock()
			DPrintf("End Get key: %s\n", key)
			return reply.Value
		} else if reply.Err == ErrNoKey {
			DPrintf("End Get key: %s\n", key)
			return ""
		} else {
			DPrintf("Error %s\n", reply.Err)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	start := time.Now()
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Idx = atomic.AddUint64(&ck.idx, 1)
	DPrintf("Begin %s key: %s, value: %s, idx: %d\n", op, key, value, args.Idx)
	leader := ck.leader
	for ; ; {
		var reply PutAppendReply
		//DPrintf("Put key %s to %d\n", key, leader)
		if !ck.servers[leader].Call("KVServer.PutAppend", &args, &reply) {
			//DPrintf("Put key %s to %d failed\n", key, leader)
			time.Sleep(time.Duration(20) * time.Millisecond)
			leader = (leader + 1) % len(ck.servers)
			continue
		}
		if reply.WrongLeader {
			leader = (leader + 1) % len(ck.servers)
			//DPrintf("Error %s, leader: %d, %d\n", reply.Err, leader, ck.leader)
		} else if reply.Err == OK {
			break;
		} else {
			//DPrintf("Error %s\n", reply.Err)
		}
	}
	ck.mu.Lock()
	ck.leader = leader
	ck.mu.Unlock()
	CalcRuntime(start, "PutAppend")
	DPrintf("End Put key: %s, idx: %d\n", key, args.Idx)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
