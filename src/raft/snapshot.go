package raft

import "bytes"
import "labgob"

type Snapshot struct {
	Index         int
	Term          int
	Size          int
	Data          []byte
}


func MakeSnapshot(data []byte) *Snapshot {
	s := &Snapshot{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&s.Index)
	d.Decode(&s.Term)
	d.Decode(&s.Size)
	d.Decode(&s.Data)
	return s
}
