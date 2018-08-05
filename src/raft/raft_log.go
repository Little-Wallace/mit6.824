package raft

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

func (log *UnstableLog) IsUpToDate(Index int, Term int) bool {
	return Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex())
}

func (log *UnstableLog) GetUnApplyEntry() []Entry {
	return log.Entries[log.applied + 1 : log.commited + 1]
}

