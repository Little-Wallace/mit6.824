package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
	"time"
	"fmt"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	idx     int64
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
	ck.idx = nrand()
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
	args.Idx = int(atomic.AddInt64(&ck.idx, 1))
	args.Key = key
	for i := 0; ; {
		var reply GetReply
		if !ck.servers[i].Call("KVServer.Get", &args, &reply) {
			time.Sleep(time.Duration(20) * time.Millisecond)
			continue
		}
		if reply.WrongLeader {
			leader := GetLeader(string(reply.Err))
			if leader != -1 {
				i = leader
			} else {
				i = 0
				time.Sleep(time.Duration(20) * time.Millisecond)
			}
		} else if reply.Err == ""{
			return reply.Value
		} else {
			fmt.Printf("Error %v\n", reply.Err)
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
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Idx = int(atomic.AddInt64(&ck.idx, 1))
	for i := 0; ; {
		var reply PutAppendReply
		if !ck.servers[i].Call("KVServer.PutAppend", &args, &reply) {
			time.Sleep(time.Duration(20) * time.Millisecond)
			continue
		}
		if reply.WrongLeader {
			leader := GetLeader(string(reply.Err))
			if leader != -1 {
				i = leader
			} else {
				i = 0
				time.Sleep(time.Duration(20) * time.Millisecond)
			}
		} else if reply.Err == ""{
			break;
		} else {
			fmt.Printf("Error %v\n", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


