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
}

func (log *UnstableLog) GetDataIndex() int {
	//return log.Entries[log.commited].DataIndex
	return log.Entries[log.size - 1].DataIndex
}

func (log *UnstableLog) GetLastIndex() int {
	return log.Entries[log.size - 1].Index
}

func (log *UnstableLog) GetLastTerm() int {
	return log.Entries[log.size - 1].Term
}

func (log *UnstableLog) Append(e Entry) {
	if e.Index >= len(log.Entries) {
		log.Entries = append(log.Entries, e)
	} else {
		log.Entries[e.Index] = e
	}
	log.size = e.Index + 1
	if e.Term < log.Entries[e.Index - 1].Term {
		fmt.Printf("================ERROR Append a term(%d) in index(%d), which prev term is %d\n",
			e.Term, e.Index, log.Entries[e.Index - 1].Term)
	}
}

func (log *UnstableLog) FindConflict(entries []Entry) int {
	for _, e := range entries {
		if e.Index >= log.size || log.Entries[e.Index].Term != e.Term {
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

func (log *UnstableLog) GetEntries(since int) []Entry {
	return log.Entries[since : log.size]
}

func (log *UnstableLog) MaybeCommit(index int) bool {
	if index > log.commited && index < len(log.Entries){
		log.commited = index
		return true
	}
	return false
}
