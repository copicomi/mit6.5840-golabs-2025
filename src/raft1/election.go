package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) SendHeartbeat() {
	args := &AppendEntriesArgs{
		term: rf.currentTerm,
		leaderId: rf.me,
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

func (rf *Raft) StartElection() {
	args := &RequestVoteArgs{ 
		term: rf.currentTerm,
		candidateId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}
}

func (rf *Raft) CheckNewLeader(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		if term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = term
			rf.lastHeartbeatTime = time.Now()
			return true
		}
	} else if rf.state == Candidate {
		if term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = term
			rf.lastHeartbeatTime = time.Now()
			return true
		}
	}
	return false
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == Leader {
			go rf.SendHeartbeat();
			// leader 固定间隔发送心跳
			time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
		} else if state == Follower || state == Candidate { 
			heartbeatTimeout := time.Duration(rf.electionTimeout)*time.Millisecond 
			if time.Since(rf.lastHeartbeatTime) > heartbeatTimeout{
				go rf.StartElection()
			}
			// follower 随机等待一段时间
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} 

	}
}