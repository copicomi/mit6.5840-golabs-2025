package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for rf.killed() == false { 
		sleepMs := 10
		rf.mu.Lock()
		rf.UpdateCommitIndex()
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entry := rf.log[i]
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i,
				}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
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
	if rf.log[l].Term == rf.currentTerm {
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