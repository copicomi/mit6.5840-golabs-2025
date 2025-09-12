package raft

import (
	"time"
)

func (rf *Raft) SendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// mDebug(rf, "Send heartbeat...")
	rf.lastHeartbeatTime = time.Now()
	rf.UpdateCommitIndex()
	rf.BoardcastHeartbeat()
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.incTermWithoutLock()
	rf.ChangeRoleWithoutLock(Candidate, rf.currentTerm)
	rf.lastHeartbeatTime = time.Now()
	  // mDebug(rf, "start election...")
	rf.BoardcastRequestVote()
	rf.persist()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		electionTimeout := GetRand(600, 800)
		heartbeatTimeout := time.Duration(electionTimeout)*time.Millisecond 
		if state == Leader { // 发送心跳
			go rf.SendHeartbeat();
		} else if state == Follower || state == Candidate { // 等待心跳
			if time.Since(rf.lastHeartbeatTime) > heartbeatTimeout{
				go rf.StartElection()
			}
		} 
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
