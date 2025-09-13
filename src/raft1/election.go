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
	   mDebug(rf, "start election...")
	rf.BoardcastRequestVote()
	rf.persist()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		electionTimeout := GetRand(400, 700)
		heartbeatTimeout := time.Duration(electionTimeout)*time.Millisecond 
		var sleepMs int
		if state == Leader { // 发送心跳
			go rf.SendHeartbeat();
			sleepMs = 100
		} else if state == Follower || state == Candidate { // 等待心跳
			if time.Since(rf.lastHeartbeatTime) > heartbeatTimeout{
				go rf.StartElection()
			}
			sleepMs = GetRand(50, 150)
		} 
		rf.mu.Unlock()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}
