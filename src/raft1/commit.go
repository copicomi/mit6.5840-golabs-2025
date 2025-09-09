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
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry := rf.log[i]
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i,
		}
	}
	// mDebug(rf, "Apply %d logs", rf.commitIndex - rf.lastApplied)
	rf.lastApplied = rf.commitIndex
	rf.applyCond.Broadcast()
}

func (rf *Raft) UpdateCommitIndex() {
	l := rf.commitIndex
	r := rf.lastLogIndex
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
	if rf.log[l].Term == rf.currentTerm {
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

func (rf *Raft) WaitUntilApplyed(index int) {
	done := make(chan bool, 1)
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		for rf.killed() == false && rf.lastApplied < index {
			rf.applyCond.Wait()
		}
		done <- true
	}()
	select {
		case <-done:
		case <-time.After(1000 * time.Millisecond):
			return
	}
}