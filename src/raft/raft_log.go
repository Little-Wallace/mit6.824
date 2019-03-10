package raft

import "fmt"

type Entry struct {
	Data []byte
	Term int
	Index int
}

type UnstableLog struct {
	Entries		[]Entry
	commited	int
	applied		int
	pk			int
}

func (log *UnstableLog) GetLastIndex() int {
	return log.Entries[len(log.Entries) - 1].Index
}

func (log *UnstableLog) GetLastTerm() int {
	return log.Entries[len(log.Entries) - 1].Term
}

func (log *UnstableLog) Append(e Entry) {
	if e.Index >= len(log.Entries) {
		log.Entries = append(log.Entries, e)
	} else {
		log.Entries[e.Index] = e
	}
}

func (log *UnstableLog) FindConflict(entries []Entry) int {
	sz := len(log.Entries)
	for _, e := range entries {
		if e.Index >= sz || log.Entries[e.Index].Term != e.Term {
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
	return log.Entries[log.applied + 1 : log.commited + 1]
}


func (log *UnstableLog) MaybeCommit(index int) bool {
	if index > log.commited {
		log.commited = index
		return true
	}
	return false
}
