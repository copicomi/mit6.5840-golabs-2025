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
	return nil
}