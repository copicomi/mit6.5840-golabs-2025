package raft

import "time"

func (rf *Raft) SendAndHandleRPC(
	server int, 
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
) {
	args := argsFactory()
	reply := replyFactory()
	for true {
		ok := sendFunction(server, args, reply)
		if ok {
			if handleFunction != nil {
				handleFunction(server, args, reply)
			}		
			break
		}
		time.Sleep(10 * time.Millisecond)
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

func (rf *Raft) BoardcastRequestVote() {
	// mDebug(rf, "Boardcast RequestVote RPC")
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCRequestVote, &RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.lastLogIndex,
			LastLogTerm: rf.log[rf.lastLogIndex].Term,
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
			Entries: []LogEntry{},
			PrevLogIndex: rf.lastLogIndex,
			PrevLogTerm: rf.log[rf.lastLogIndex].Term,
		}),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}