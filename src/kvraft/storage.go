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
	kv := make(map[string]string)
	if err := d.Decode(&s.kv); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = kv
	return nil
}

func (s *Storage) Bytes() []byte {
	s.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s.kv)
	s.mu.Unlock()
	return w.Bytes()
}

func (s *Storage) Put(idx uint64, key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok :=	s.commands[idx]; ok {
		return
	}
	s.kv[key] = value
	s.commands[idx] = time.Now()

}

