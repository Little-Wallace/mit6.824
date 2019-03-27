package raftkv

import (
	"sync"
	"time"
	"raft"
	"bytes"
	"labgob"
)

type Storage struct {
	mu      sync.Mutex
	kv		map[string]string
	commands map[uint64]time.Time
	// Your definitions here.
}

func (s *Storage) Get(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.kv[key]; ok {
		return v
	}
	return ""


}

func (s *Storage) CheckCommand(idx uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok :=	s.commands[idx]; ok {
		return true
	}
	return false
}

func (s *Storage) ApplySnapshot(snap *raft.Snapshot) error {
	r := bytes.NewBuffer(snap.Data)
	d := labgob.NewDecoder(r)
	var size int
	d.Decode(&size)
	arrs := make([]string, size)
	if err := d.Decode(&arrs); err != nil {
		DPrintf("recover failed\n")
		return err
	}
	var ts []uint64
	d.Decode(&ts)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < size; i += 2 {
		s.kv[arrs[i]] = arrs[i + 1]
		if arrs[i] < "3" && len(arrs[i]) <= 2 {
			DPrintf("recover: kv[%s]=%s\n", arrs[i], arrs[i + 1])
		}
	}
	for _, k := range ts {
		s.commands[k] = time.Now()
		//DPrintf("recover: commands[%d]\n", k)
	}
	return nil
}

func (s *Storage) Bytes() []byte {
	s.mu.Lock()
	size := 0
	arrs := make([]string, len(s.kv) * 2)
	for k, v := range s.kv {
		arrs[size] = k
		size ++
		arrs[size] = v
		size ++
		if k < "3" && len(k) <= 2 {
			DPrintf("store: kv[%s]=%s\n", k, v)
		}
	}
	DPrintf("store a map , size : %d, arra len: %d, size: %d\n", len(s.kv), len(arrs), size)
	now := time.Now()
	ts := make([]uint64, 0)
	for k, v := range s.commands {
		if now.Sub(v).Seconds() > 20 {
			continue
		}
		//DPrintf("store: commands[%d]\n", k)
		ts = append(ts, k)
	}
	s.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(size)
	e.Encode(arrs)
	e.Encode(ts)
	return w.Bytes()
}

func (s *Storage) Put(idx uint64, key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok :=	s.commands[idx]; ok {
		DPrintf("skip put key: %s, value: %s, idx: %d\n", key, value, idx)
		return
	}
	now := time.Now()
	s.kv[key] = value
	s.commands[idx] = now
	delete(s.commands, idx - 1)
	delete(s.commands, idx - 2)
}

