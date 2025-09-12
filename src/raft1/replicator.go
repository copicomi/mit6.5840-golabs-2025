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
	if rf.state != Leader {
		return
	}
	if rf.GetLastLogIndexWithoutLock() >= rf.nextIndex[server] {
		if rf.IsExistedInSnapshot(rf.nextIndex[server]) {
			rf.replicateInstallSnapshot(server)
		} else {
			rf.replicateAppendEntries(server)
		}
	} 
}

func (rf *Raft) replicateAppendEntries(server int) {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.GetLogTermAtIndexWithoutLock(rf.nextIndex[server] - 1),
		Entries:      rf.GetLogListBeginAtIndexWithoutLock(rf.nextIndex[server]),
		LeaderCommit: rf.commitIndex,
	}
	mDebug(rf, "send APPEND ENTRIES RPC to server %d, prev %d, loglen %d, term %d", server, args.PrevLogIndex, len(args.Entries), args.Term)
	// mDebug(rf, "replicate %d at %d", server, args.PrevLogIndex)
	go rf.SendAndHandleRPC(
		server,
		rf.MakeArgsFactoryFunction(RPCAppendEntries, args),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		rf.MakeHandleFunction(RPCAppendEntries),
	)
}

func (rf *Raft) replicateInstallSnapshot(server int) { 
	mDebugLock(rf, "send InstallSnapshot ")
	args := &InstallSnapshotArgs{
		Data: rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.snapshotEndIndex,
		LastIncludedTerm: rf.GetSnapshotEndTermWithoutLock(),
		LeaderId: rf.me,
		Term: rf.currentTerm,
	}
	go rf.SendAndHandleRPC(
		server,
		rf.MakeArgsFactoryFunction(RPCInstallSnapshot, args),
		rf.MakeEmptyReplyFactoryFunction(RPCInstallSnapshot),
		rf.MakeSendFunction(RPCInstallSnapshot),
		rf.MakeHandleFunction(RPCInstallSnapshot),
	)
}
func (rf *Raft) BackforwardsNextIndex(server int) {
	i := rf.nextIndex[server] - 1
	mDebug(rf, "BackforwardsNextIndex with i %d", i)
	conflictTerm := rf.GetLogTermAtIndexWithoutLock(i)
	for ; i > rf.snapshotEndIndex; i -- {
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