package raft

func (rf *Raft) MakeSendFunction(rpcType RPCType) RPCSendFunc {
	if rpcType == RPCAppendEntries {
		return func (server int, args interface{}, reply interface{}) bool {
			return rf.sendAppendEntries(server, args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
		}
	}
	if rpcType == RPCRequestVote {
		return func (server int, args interface{}, reply interface{}) bool {
			return rf.sendRequestVote(server, args.(*RequestVoteArgs), reply.(*RequestVoteReply))
		}
	}
	if rpcType == RPCInstallSnapshot {
		return func (server int, args interface{}, reply interface{}) bool {
			return rf.sendInstallSnapshot(server, args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply))
		}
	}
	return nil
}

func (rf *Raft) MakeHandleFunction(rpcType RPCType) RPCHandleFunc {
	if rpcType == RPCAppendEntries {
		return func (server int, args interface{}, reply interface{}) {
			appendReply, ok := reply.(*AppendEntriesReply)
			appendArgs, okArgs := args.(*AppendEntriesArgs)
			if !ok || !okArgs {
				return;
			}
			rf.HandleAppendReply(server, appendArgs, appendReply)
		}
	}
	if rpcType == RPCRequestVote {
		return func (server int, args interface{}, reply interface{}) {
			voteReply, ok := reply.(*RequestVoteReply)
			voteArgs, okArgs := args.(*RequestVoteArgs)
			if !ok || !okArgs {
				return;
			}
			rf.HandleVoteReply(server, voteArgs, voteReply)
		}
	}
	if rpcType == RPCInstallSnapshot { 
		return func (server int, args interface{}, reply interface{}) {
			installReply, ok := reply.(*InstallSnapshotReply)
			installArgs, okArgs := args.(*InstallSnapshotArgs)
			if !ok || !okArgs {
				return;
			}
			rf.HandleInstallReply(server, installArgs, installReply)
		}
	}
	return nil
}

func (rf *Raft) MakeEmptyReplyFactoryFunction(rpcType RPCType) RPCFactoryFunc {
	if rpcType == RPCAppendEntries {
		return func() interface{} {
			return &AppendEntriesReply{}
		}
	}
	if rpcType == RPCRequestVote {
		return func() interface{} {
			return &RequestVoteReply{}
		}
	}
	if rpcType == RPCInstallSnapshot {
		return func() interface{} {
			return &InstallSnapshotReply{}
		}
	}
	return nil
}

func (rf *Raft) MakeArgsFactoryFunction(rpcType RPCType, args interface{}) RPCFactoryFunc {
	if rpcType == RPCAppendEntries {
		return func() interface{} {
			return args.(*AppendEntriesArgs)
		}
	}
	if rpcType == RPCRequestVote {
		return func() interface{} {
			return args.(*RequestVoteArgs)
		}
	}
	if rpcType == RPCInstallSnapshot {
		return func() interface{} {
			return args.(*InstallSnapshotArgs)
		}
	}
	return nil
}