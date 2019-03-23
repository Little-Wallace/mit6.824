package raft

import "bytes"
import "labgob"

type Snapshot struct {
	Index         int
	DataIndex     int
	Term          int
	Data          []byte
}


func MakeSnapshot(data []byte) *Snapshot {
	s := &Snapshot{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&s.Index)
	d.Decode(&s.DataIndex)
	d.Decode(&s.Term)
	d.Decode(&s.Data)
	return s
}

func (s *Snapshot) Bytes() []byte {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(s.Index)
	e.Encode(s.DataIndex)
	e.Encode(s.Term)
	e.Encode(s.Data)
    return w.Bytes()
}
