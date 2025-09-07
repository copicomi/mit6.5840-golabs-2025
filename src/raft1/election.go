package raft

import (
	"time"
)

func (rf *Raft) SendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
		}(i)
	}
	rf.lastHeartbeatTime = time.Now()
}

func (rf *Raft) AskForVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.sendRequestVote(server, args, reply)
	if reply.VoteGranted {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Candidate || rf.currentTerm != args.Term {
			return
		}
		rf.incVoteCountWithoutLock()
		if rf.voteCount > len(rf.peers) / 2 {
			rf.ChangeRoleWithoutLock(Leader, rf.currentTerm)
			go rf.SendHeartbeat()
		}
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.incTermWithoutLock()
	rf.ChangeRoleWithoutLock(Candidate, rf.currentTerm)
	args := &RequestVoteArgs{ 
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go rf.AskForVote(i, args, reply)
	}
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
			sleepMs = GetRand(50, 350)
		} 
		rf.mu.Unlock()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
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