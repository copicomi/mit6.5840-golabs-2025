package raft

type LogEntry struct {
	Term    int
	Command interface{}
}
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.lastLogIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (3B).
	if isLeader {
		go rf.AppendAndReplicationSingleCommand(command)
	}  
	// TODO(3B): 注意，这里需要等到成功 Apply 之后才能返回 client
	return index, term, isLeader
}

func (rf *Raft) AppendAndReplicationSingleCommand(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log := LogEntry{
		Command: command,
		Term: rf.currentTerm,
	}
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		Entries: []LogEntry{log},
		PrevLogIndex: rf.lastLogIndex,
		PrevLogTerm: rf.lastLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	rf.AppendSingleLogWithoutLock(log)
	rf.Boardcast(
		rf.MakeArgsFactoryFunction(RPCAppendEntries, args),
		rf.MakeEmptyReplyFactoryFunction(RPCAppendEntries),
		rf.MakeSendFunction(RPCAppendEntries),
		nil,
	)
}

func (rf *Raft) AppendSingleLogWithoutLock(log LogEntry) {
	rf.log = append(rf.log, log)
	rf.lastLogIndex ++
	rf.lastLogTerm = log.Term
	rf.persist()
}

func (rf *Raft) CutLogListWithoutLock(index int) {
	if index < rf.lastLogIndex {
		rf.log = rf.log[:index]
		rf.lastLogIndex = index
		if index == 0 {
			rf.lastLogTerm = 0
		}
	}
	rf.persist()
}

func (rf *Raft) AppendLogListWithoutLock(logs []LogEntry, prevLogIndex int) {
	for i, entry := range logs {
		logIndex := prevLogIndex + 1 + i
		if logIndex <= rf.lastLogIndex {
			if rf.log[logIndex].Term != entry.Term {
				rf.CutLogListWithoutLock(logIndex)
			}
		}
		rf.AppendSingleLogWithoutLock(entry)
	}
}