package raft

import "fmt"

type Entry struct {
	//Data []byte
	Data interface{}
	Term int
	Index int
	DataIndex int
}

type UnstableLog struct {
	Entries		[]Entry
	commited	int
	applied		int
	size		int
	snapshot    *Snapshot
}

func (log *UnstableLog) Size() int {
	return log.size
}

func (log* UnstableLog) SetSnapshot(snapshot *Snapshot) {
	prevIndex := -1
	if log.snapshot != nil {
		prevIndex = log.snapshot.Index
	}
	var entries []Entry
	if log.size - prevIndex - 1 > 0 {
		entries = log.Entries[:log.size - prevIndex - 1]
	}
	log.snapshot = snapshot
	log.size = snapshot.Index + 1
	for _, e := range entries {
		if e.Index <= log.snapshot.Index {
			continue
		}
		log.Append(e)
	}
}

func (log *UnstableLog) MatchIndexAndTerm(index int, term int) bool {
	if index >= log.size {
		return false
	}
	if log.snapshot != nil {
		if index < log.snapshot.Index {
			return false
		}
		if index == log.snapshot.Index {
			return true
		}
		return log.Entries[index - log.snapshot.Index - 1].Term == term
	}
	return log.Entries[index].Term == term
}

func (log *UnstableLog) GetEntry(idx int) *Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}
	return &log.Entries[idx - prevSize]
}

func (log *UnstableLog) GetDataIndex() int {
	return log.GetEntry(log.size - 1).DataIndex
}

func (log *UnstableLog) GetLastIndex() int {
	return log.GetEntry(log.size - 1).Index
}

func (log *UnstableLog) GetLastTerm() int {
	return log.GetEntry(log.size - 1).Term
}

func (log *UnstableLog) Append(e Entry) {
	idx := e.Index
	if log.snapshot != nil {
		idx -= log.snapshot.Index + 1
	}
	if idx >= len(log.Entries) {
		log.Entries = append(log.Entries, e)
	} else {
		log.Entries[idx] = e
	}
	log.size = e.Index + 1
}

func (log *UnstableLog) FindConflict(entries []Entry) int {
	for _, e := range entries {
		if e.Index >= log.size || !log.MatchIndexAndTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

func (log *UnstableLog) IsUpToDate(Index int, Term int) bool {
	ans := false
//	return Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex())
	if Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex()) {
		ans = true
	}
	fmt.Printf("len: %d, term: %d, last term: %d, index: %d, lastIndex: %d, result: %t\n", len(log.Entries), Term, log.GetLastTerm(),
		Index, log.GetLastIndex(), ans)
	return ans
}

func (log *UnstableLog) GetUnApplyEntry() []Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}

	return log.Entries[log.applied + 1 - prevSize : log.commited + 1 - prevSize]
}

func (log *UnstableLog) GetEntries(since int) []Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}
	return log.Entries[since - prevSize : log.size - prevSize]
}

func (log *UnstableLog) MaybeCommit(index int) bool {
	if index > log.commited && index < log.size{
		log.commited = index
		return true
	}
	return false
}
