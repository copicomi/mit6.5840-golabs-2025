package raft

func (rf *Raft) SendAndHandleRPC(
	server int, 
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
) {
	args := argsFactory()
	reply := replyFactory()
	sendFunction(server, args, reply)
	if handleFunction != nil {
		handleFunction(server, args, reply)
	}
}
func (rf *Raft) BoardcastWithPeersIndex(
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
	peersIndex []int,
) {
	for _, i := range peersIndex {
		go rf.SendAndHandleRPC(
			i, 
			argsFactory, 
			replyFactory, 
			sendFunction, 
			handleFunction,
		)
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
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.lastLogIndex >= rf.nextIndex[i] {
			rf.SendAndHandleRPC(
				i,
				rf.MakeArgsFactoryFunction(RPCAppendEntries, &AppendEntriesArgs{
					Entries: logs,
					LeaderId: rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm: rf.log[rf.nextIndex[i] - 1].Term,
				}),
				rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
				rf.MakeSendFunction(RPCAppendEntries),
				rf.MakeHandleFunction(RPCAppendEntries),
			)
		}
	}
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
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}