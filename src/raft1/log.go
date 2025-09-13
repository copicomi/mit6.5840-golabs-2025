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
	index := rf.GetLastLogIndexWithoutLock() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (3B).
	if isLeader {
		log := LogEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		mDebug(rf, "start one(%v)", log.Command)
		rf.AppendLogListWithoutLock([]LogEntry{log}, rf.GetLastLogIndexWithoutLock())
		go rf.WakeupAllReplicators()
	}  
	return index, term, isLeader
}

func (rf *Raft) AppendSingleLogWithoutLock(log LogEntry) {
	rf.log = append(rf.log, log)
	rf.persist()
	// mDebug(rf, "Append 1 log, len = %d, lastLogIndex = %d", len(rf.log), rf.lastLogIndex)
}

func (rf *Raft) CutLogListWithoutLock(index int) {
	// panic(index > len(rf.log))
	index = index - rf.snapshotEndIndex
	rf.log = rf.log[:index]
	rf.persist()
	// mDebug(rf, "Cut log list, len = %d, lastLogIndex = %d", len(rf.log), rf.GetLastLogIndexWithoutLock())
}

func (rf *Raft) AppendLogListWithoutLock(logs []LogEntry, prevLogIndex int) {
	if prevLogIndex == 0 {
		mDebug(rf, "Append %d logs, lastLogIndex = %d", len(logs), rf.GetLastLogIndexWithoutLock())
	}
	for i, entry := range logs {
		logIndex := prevLogIndex + 1 + i
		if logIndex <= rf.GetLastLogIndexWithoutLock(){
			if rf.GetLogTermAtIndexWithoutLock(logIndex) != entry.Term {
				rf.CutLogListWithoutLock(logIndex)
			} else {
				continue
			}
		}
		rf.AppendSingleLogWithoutLock(entry)
	}
	if prevLogIndex == 0 {
		mDebug(rf, "Append %d logs, lastLogIndex = %d", len(logs), rf.GetLastLogIndexWithoutLock())
		mDebugIndex(rf, "append log list")
	}
	if len(logs) > 0 {
		mDebug(rf, "Append %d logs, lastLogIndex = %d", len(logs), rf.GetLastLogIndexWithoutLock())
	}
}

