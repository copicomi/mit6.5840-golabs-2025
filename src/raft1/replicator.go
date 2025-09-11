package raft

func (rf *Raft) replicator(server int) { 
	for !rf.killed() { 
		rf.replicateCond[server].L.Lock()
		rf.replicateCond[server].Wait()
		if rf.killed() {
			return
		}
		rf.replicateOneRound(server)
		rf.replicateCond[server].L.Unlock()
	}
}

func (rf *Raft) replicateOneRound(server int) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	mDebugLock(rf, "replicateOneRound")
	if rf.state != Leader {
		return
	}
	if rf.GetLastLogIndexWithoutLock() >= rf.nextIndex[server] {
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.GetLogTermAtIndexWithoutLock(rf.nextIndex[server] - 1),
			Entries:      rf.GetLogListBeginAtIndexWithoutLock(rf.nextIndex[server]),
			LeaderCommit: rf.commitIndex,
		}
		// mDebug(rf, "replicate %d at %d", server, args.PrevLogIndex)
		go rf.SendAndHandleRPC(
			server,
			rf.MakeArgsFactoryFunction(RPCAppendEntries, args),
			rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
			rf.MakeSendFunction(RPCAppendEntries),
			rf.MakeHandleFunction(RPCAppendEntries),
		)
	} 
}

func (rf *Raft) BackforwardsNextIndex(server int) {
	i := rf.nextIndex[server] - 1
	conflictTerm := rf.GetLogTermAtIndexWithoutLock(i)
	for ; i > rf.firstLogIndex; i-- {
		if rf.GetLogTermAtIndexWithoutLock(i) != conflictTerm {
			break
		}
	}
	rf.nextIndex[server] = i + 1
	// mDebug(rf, "BackforwardsNextIndex %d", rf.nextIndex[server])
}

func (rf *Raft) WakeupAllReplicators() { 
	for i := 0; i < len(rf.peers); i++ {
		go rf.replicateCond[i].Signal()
	}
}