package raft

import (
	"time"
)

func (rf *Raft) SendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	me := rf.me
	rf.lastHeartbeatTime = time.Now()

	args := &AppendEntriesArgs{
		Term: term,
		LeaderId: me,
	}

	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCAppendEntries, args),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%d StartElection", rf.me)
	rf.incTermWithoutLock()
	rf.ChangeRoleWithoutLock(Candidate, rf.currentTerm)
	rf.lastHeartbeatTime = time.Now()

	term := rf.currentTerm
	me := rf.me
	args := &RequestVoteArgs{
		Term: term,
		CandidateId: me,
	}
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCRequestVote, args),
		rf.MakeEmptyReplyFactoryFunction(RPCRequestVote),
		rf.MakeSendFunction(RPCRequestVote),
		rf.MakeHandleFunction(RPCRequestVote),
	)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		var sleepMs int
		state := rf.state
		heartbeatTimeout := time.Duration(rf.electionTimeout)*time.Millisecond 
		if state == Leader { // 发送心跳
			go rf.SendHeartbeat();
			sleepMs = rf.heartbeatInterval
		} else if state == Follower || state == Candidate { // 等待心跳
			if time.Since(rf.lastHeartbeatTime) > heartbeatTimeout{
				go rf.StartElection()
			}
			sleepMs = GetRand(400, 650)
		} 
		rf.mu.Unlock()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}
