package raftkv

import (
	"strconv"
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



func GetLeader(data string) int {
	if leader, err := strconv.Atoi(data); err == nil {
		return leader
	}
	return -1
}

func WriteLeader(leader int) string {
	return strconv.Itoa(leader)
}

