package raft

func (rf *Raft) replicator(server int) { 
	for !rf.killed() { 
		rf.replicateCond[server].L.Lock()
		rf.replicateCond[server].Wait()
		rf.replicateCond[server].L.Unlock()
		if rf.killed() {
			return
		}
		rf.replicateOneRound(server)
	}
}

func (rf *Raft) replicateOneRound(server int) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	if rf.lastLogIndex >= rf.nextIndex[server] {
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		go rf.SendAndHandleRPC(
			server,
			rf.MakeArgsFactoryFunction(RPCAppendEntries, args),
			rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
			rf.MakeSendFunction(RPCAppendEntries),
			rf.MakeHandleFunction(RPCAppendEntries),
		)
	} 
}

func (rf *Raft) WakeupAllReplicators() { 
	for i := 0; i < len(rf.peers); i++ {
		rf.replicateCond[i].Broadcast()
	}
}