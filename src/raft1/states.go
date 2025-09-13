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
			rf.nextIndex[i] = rf.GetLastLogIndexWithoutLock() + 1
			rf.matchIndex[i] = rf.snapshotEndIndex
		}
		rf.WakeupAllReplicators()
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
	rf.snapshotEndIndex = 0
	rf.replicateCond = make([]*sync.Cond, len(rf.peers))
	for i := range rf.peers {
		rf.replicateCond[i] = sync.NewCond(&sync.Mutex{})
	}
}

func (rf *Raft) GetLastLogIndexAndTermWithoutLock() (int, int){
	lastLogIndex := rf.GetLastLogIndexWithoutLock()
	lastLog, _ := rf.GetLogAtIndexWithoutLock(lastLogIndex)
	return lastLogIndex, lastLog.Term
}

func (rf *Raft) GetLastLogTermWithoutLock() int {
	lastLogIndex := rf.GetLastLogIndexWithoutLock()
	lastLog, _ := rf.GetLogAtIndexWithoutLock(lastLogIndex)
	return lastLog.Term
}
func (rf *Raft) GetLastLogIndexWithoutLock() int {
	return len(rf.log) + rf.snapshotEndIndex - 1
}

func (rf *Raft) GetLogAtIndexWithoutLock(index int) (LogEntry, bool) {
	index = index - rf.snapshotEndIndex
	return rf.log[index], true
}
func (rf *Raft) GetLogTermAtIndexWithoutLock(index int) int {
	log, ok := rf.GetLogAtIndexWithoutLock(index)
	if !ok {
		return -1
	}
	return log.Term
}

func (rf *Raft) GetLogListBeginAtIndexWithoutLock(index int) []LogEntry {
	index = index - rf.snapshotEndIndex
    return rf.log[index:]
}

func (rf *Raft) GetSnapshotEndTermWithoutLock() int {
	return rf.log[0].Term
}
func (rf *Raft) IsNewerThan(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.GetLastLogIndexAndTermWithoutLock()
	if lastLogTerm > term {
		return true
	} else if lastLogTerm == term && lastLogIndex > index {
		return true
	}
	return false
}
func (rf *Raft) IsVotedForOthers(server int) bool {
	return rf.votedFor != server && rf.votedFor != Nobody
}

func (rf *Raft) IsFoundAnotherLeader(term int) bool {
	if term == rf.currentTerm && rf.state == Candidate {
		return false // 同时期候选人不能改变 state
	}
	if term > rf.currentTerm {
		return true
	}
	return false
}

func (rf *Raft) IsFoundAnotherCandidateInSameTerm(term int) bool {
	return term == rf.currentTerm && rf.state == Candidate
}

func (rf *Raft) IsOutofDate(term int) bool {
	return term < rf.currentTerm
}

func (rf *Raft) IsMatchPrevLog(index int, term int) bool {
	if rf.GetLastLogIndexWithoutLock() < index {
		return false
	}
	if rf.GetLogTermAtIndexWithoutLock(index) != term {
		mDebug(rf, "Log term mismatch at %d, %d, %d", index, rf.GetLogTermAtIndexWithoutLock(index), term)
		return false
	}
	return true
}

func (rf *Raft) IsExistedInLogList(index int) bool {
	return index <= rf.GetLastLogIndexWithoutLock() &&
			index > rf.snapshotEndIndex
}

func (rf *Raft) IsExistedInSnapshot(index int) bool {
	// 必须判断 snapshot是否存在，否则在 snapshot 为空时，replicator 会尝试发送空快照
	return index <= rf.snapshotEndIndex && index >= 0 && rf.snapshotEndIndex > 0
}

func (rf *Raft) UpdateServerMatchIndex(server int, index int) {
	rf.nextIndex[server] = max(rf.nextIndex[server], index + 1)
	rf.matchIndex[server] = max(rf.matchIndex[server], index)
}