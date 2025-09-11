package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for rf.killed() == false { 
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.ApplyCommittedLogs()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ApplyCommittedLogs() {
	commitIndex := rf.commitIndex
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry, _ := rf.GetLogAtIndexWithoutLock(i)
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i,
		}
	}
	// mDebug(rf, "Apply %d logs", rf.commitIndex - rf.lastApplied)
	rf.lastApplied = max(rf.lastApplied, commitIndex)
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
