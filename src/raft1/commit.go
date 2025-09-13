package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for rf.killed() == false { 
		rf.applyMu.Lock()
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.ApplyCommittedLogs()
		}
		rf.mu.Unlock()
		rf.applyMu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ApplyCommittedLogs() {
	last := rf.lastApplied
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		i = max(i, rf.lastApplied)
		if i > rf.commitIndex {
			rf.mu.Lock()
			return
		}
		entry, _ := rf.GetLogAtIndexWithoutLock(i)
		rf.lastApplied = max(rf.lastApplied, i)
		rf.mu.Unlock()
		mDebug(rf, "Apply 1 log to %d", i)
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i,
		}
		rf.mu.Lock()
	}
	mDebug(rf, "Apply %d logs to %d", rf.commitIndex - last, rf.commitIndex)
}

func (rf *Raft) UpdateCommitIndex() {
	l := rf.commitIndex
	r := rf.GetLastLogIndexWithoutLock()
	for l < r {
		mid := (l + r + 1) / 2
		if rf.IsReadyToCommit(mid) {
			l = mid
		} else {
			r = mid - 1
		}
	}
	if l == rf.commitIndex {
		return
	}
	if rf.GetLogTermAtIndexWithoutLock(l) == rf.currentTerm {
		mDebug(rf, "Update commit index to %d", l)
		rf.commitIndex = l
	}
}

func (rf *Raft) IsReadyToCommit(index int) bool {
	count := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= index {
			count ++
		}
	}
	return count > len(rf.peers)/2
}
