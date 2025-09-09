package raft

func (rf *Raft) BoardcastWithPeersIndex(
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
	peersIndex []int,
) {
	for _, i := range peersIndex {
		go func(server int) {
			args := argsFactory()
			reply := replyFactory()
			sendFunction(server, args, reply)
			if (handleFunction != nil) {
				handleFunction(server, args, reply)
			}
		}(i)
	}
}
func (rf *Raft) Boardcast(
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
) {
	peersIndex := []int{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		peersIndex = append(peersIndex, i)
	}
	rf.BoardcastWithPeersIndex(
		argsFactory,
		replyFactory,
		sendFunction,
		handleFunction,
		peersIndex,
	)
}

func (rf *Raft) BoardcastAppendEntries(logs []LogEntry) {
	peersIndex := []int{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.lastLogIndex >= rf.nextIndex[i] {
			peersIndex = append(peersIndex, i)
		}
	}
	mDebug(rf, "Boardcast Append RPC with leaderCommit %d", rf.commitIndex)
	rf.BoardcastWithPeersIndex(
		rf.MakeArgsFactoryFunction(RPCAppendEntries, &AppendEntriesArgs{
			Entries: logs,
			LeaderId: rf.me,
			Term: rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.lastLogIndex,
			PrevLogTerm: rf.lastLogTerm,
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		rf.MakeHandleFunction(RPCAppendEntries),
		peersIndex,
	)
}

func (rf *Raft) BoardcastRequestVote() {
	mDebug(rf, "Boardcast RequestVote RPC")
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCRequestVote, &RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCRequestVote),
		rf.MakeSendFunction(RPCRequestVote),
		rf.MakeHandleFunction(RPCRequestVote),
	)
}

func (rf *Raft) BoardcastHeartbeat() {
	// mDebug(rf, "Boardcast Heartbeat RPC")
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCAppendEntries, &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.lastLogIndex,
			PrevLogTerm: rf.lastLogTerm,
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}