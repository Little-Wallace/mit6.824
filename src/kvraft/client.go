package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
	"time"
	"fmt"
	"sync"
)


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
	ck.idx = uint64(nrand() % 10000 + 1) * 1000000
	ck.leader = 0
	ck.addrs = make([]int, len(servers) + 1)
	for idx, _ := range ck.addrs {
		ck.addrs[idx] = -1
	}
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
	fmt.Printf("Begin Get key: %s\n", key)
	leader := ck.leader
	for ; ; {
		var reply GetReply
		if !ck.servers[leader].Call("KVServer.Get", &args, &reply) {
			time.Sleep(time.Duration(20) * time.Millisecond)
			continue
		}
		if reply.WrongLeader {
			lastLeader := leader
			leader = ck.getLeader(string(reply.Err), leader)
			fmt.Printf("Get Key %s, wrongleader %d, change leader to %d\n", key, lastLeader, leader)
		} else if reply.Err == ""{
			return reply.Value
		} else {
			fmt.Printf("Error %s\n", reply.Err)
		}
	}

	ck.mu.Lock()
	ck.leader = leader
	ck.mu.Unlock()
	fmt.Printf("End Get key: %s\n", key)
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
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Idx = atomic.AddUint64(&ck.idx, 1)
	fmt.Printf("Begin Put key: %s, value: %s, idx: %d\n", key, value, args.Idx)
	leader := ck.leader
	for ; ; {
		var reply PutAppendReply
		//fmt.Printf("Put key %s to %d\n", key, leader)
		if !ck.servers[leader].Call("KVServer.PutAppend", &args, &reply) {
			//fmt.Printf("Put key %s to %d failed\n", key, leader)
			time.Sleep(time.Duration(20) * time.Millisecond)
			leader = (leader + 1) % len(ck.servers)
			continue
		}
		if reply.WrongLeader {
			leader = ck.getLeader(string(reply.Err), leader)
			fmt.Printf("Error %s, leader: %d, %d\n", reply.Err, leader, ck.leader)
		} else if reply.Err == "" {
			break;
		} else {
			fmt.Printf("Error %s\n", reply.Err)
		}
	}
	ck.mu.Lock()
	ck.leader = leader
	ck.mu.Unlock()
	fmt.Printf("End Put key: %s, idx: %d\n", key, args.Idx)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getLeader(data string, leader int) int {
	me, l := GetLeader(data)
	ck.mu.Lock()
	ck.addrs[me] = leader
	if l != -1 && ck.addrs[l] != -1{
		leader = ck.addrs[l]
	} else {
		leader = (leader + 1) % len(ck.servers)
	}
	ck.mu.Unlock()
	if l == -1 {
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
	return leader
}