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
	index := rf.lastLogIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (3B).
	if isLeader {
		mDebug(rf, "start begin")
		log := LogEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		rf.AppendLogListWithoutLock([]LogEntry{log}, rf.lastLogIndex)
		rf.WakeupAllReplicators()
	}  
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) AppendSingleLogWithoutLock(log LogEntry) {
	rf.log = append(rf.log, log)
	rf.lastLogIndex ++
	rf.lastLogTerm = log.Term
	rf.persist()
	// mDebug(rf, "Append 1 log, len = %d, lastLogIndex = %d", len(rf.log), rf.lastLogIndex)
}

func (rf *Raft) CutLogListWithoutLock(index int) {
	// panic(index > len(rf.log))
	rf.log = rf.log[:index]
	rf.lastLogIndex = index-1
	rf.lastLogTerm = rf.log[rf.lastLogIndex].Term
	rf.persist()
	// TODO(3B): Delete this line when complete 3B
}

func (rf *Raft) AppendLogListWithoutLock(logs []LogEntry, prevLogIndex int) {
	for i, entry := range logs {
		logIndex := prevLogIndex + 1 + i
		if logIndex <= rf.lastLogIndex {
			if rf.log[logIndex].Term != entry.Term {
				rf.CutLogListWithoutLock(logIndex)
			} else {
				continue
			}
		}
		rf.AppendSingleLogWithoutLock(entry)
	}
	// mDebug(rf, "Append %d logs, len = %d, lastLogIndex = %d", len(logs), rf.lastLogIndex, rf.lastLogIndex)
}