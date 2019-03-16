package raftkv

import (
	"sync"
	"bytes"
	"labgob"
)

type Storage struct {
	mu      sync.Mutex
	kv		map[string][]byte
	// Your definitions here.
}

func (s *Storage) Get(key string) (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.kv[key]; ok {
		//r := bytes.NewBuffer([]byte(v))
		r := bytes.NewBuffer(v)
		d := labgob.NewDecoder(r)
		var idx int
		var value string
		d.Decode(&idx)
		d.Decode(&value)
		return value, idx
	}
	return "", -1

}

func (s *Storage) Put(key string, value string, idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(idx)
	e.Encode(value)
	s.kv[key] = w.Bytes()
}

