package raftkv

import (
	"bytes"
	"labgob"
	"strings"
)

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Idx   int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Idx int
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}



func GetLeader(data string) (int, int) {
	r := strings.NewReader(data)
	d := labgob.NewDecoder(r)
	var me int
	var leader int
	d.Decode(&me)
	d.Decode(&leader)
	return me, leader
}

func WriteLeader(me int, leader int) string {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(me)
	e.Encode(leader)
	return w.String()
}

