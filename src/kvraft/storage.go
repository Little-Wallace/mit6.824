package raftkv

import (
	"sync"
	"time"
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
	//if v, ok := s.kv[key]; ok {
	//	//r := bytes.NewBuffer([]byte(v))
	//	r := bytes.NewBuffer(v)
	//	d := labgob.NewDecoder(r)
	//	var idx int
	//	var value string
	//	d.Decode(&idx)
	//	d.Decode(&value)
	//	return value, idx
	//}
	//return "", -1

}

func (s *Storage) CheckCommand(idx uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok :=	s.commands[idx]; ok {
		return true
	}
	return false
}

func (s *Storage) Put(idx uint64, key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok :=	s.commands[idx]; ok {
		return
	}
	s.kv[key] = value
	s.commands[idx] = time.Now()
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(idx)
	//e.Encode(value)
	//s.kv[key] = w.Bytes()
}

