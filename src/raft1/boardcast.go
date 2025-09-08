package raft

func (rf *Raft) Boardcast(
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
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

func (rf *Raft) BoardcastAppendEntries(logs []LogEntry) {
	rf.Boardcast(
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
	)
}

func (rf *Raft) BoardcastRequestVote() {
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
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCAppendEntries, &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}