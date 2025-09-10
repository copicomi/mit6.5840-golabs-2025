package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower  int = iota
	Candidate 
	Leader    
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) incVoteCountWithoutLock() {
	rf.voteCount ++
}

func (rf *Raft) incTermWithoutLock() {
	rf.currentTerm ++
	rf.votedFor = Nobody
	rf.voteCount = 0
}
func (rf *Raft) incTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.incTermWithoutLock()
}

func (rf *Raft) ChangeRoleWithoutLock(role int, term int) { 
	rf.state = role
	rf.currentTerm = term
	if role == Follower {
		rf.votedFor = Nobody
	} else if role == Candidate {
		rf.votedFor = rf.me
		rf.voteCount = 1
	} else if role == Leader {
		mDebug(rf, "Change to leader")
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) ChangeRole(role int, term int) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ChangeRoleWithoutLock(role, term)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) InitElectionState() {
	// 3A election
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.heartbeatInterval = 100 // heartbeat every 100 ms
	rf.lastHeartbeatTime = time.Now()
}

func (rf *Raft) InitLogState() { 
	// 3B log
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Command: nil, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.replicateCond = make([]*sync.Cond, len(rf.peers))
	for i := range rf.peers {
		rf.replicateCond[i] = sync.NewCond(&sync.Mutex{})
	}
}
func (rf *Raft) IsNewerThan(lastLogIndex int, lastLogTerm int) bool {
	// TODO(3B): Delete this line when complete 3B
	if rf.lastLogTerm > lastLogTerm {
		return true
	} else if rf.lastLogTerm == lastLogTerm && rf.lastLogIndex > lastLogIndex {
		return true
	}
	return false
}
func (rf *Raft) IsVotedForOthers(server int) bool {
	return rf.votedFor != server && rf.votedFor != Nobody
}

func (rf *Raft) IsFoundAnotherLeader(term int) bool {
	if term > rf.currentTerm {
		return true
	}
	if term == rf.currentTerm && rf.state == Candidate {
		return true
	}
	return false
}

func (rf *Raft) IsMatchPrevLog(index int, term int) bool {
	if rf.lastLogIndex < index {
		return false
	}
	if rf.log[index].Term != term {
		return false
	}
	return true
}