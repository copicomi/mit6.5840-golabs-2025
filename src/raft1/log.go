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
	index := rf.GetLastLogIndexWithoutLock() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (3B).
	if isLeader {
		log := LogEntry{
			Command: command,
			Term: rf.currentTerm,
		}
		rf.AppendLogListWithoutLock([]LogEntry{log}, rf.GetLastLogIndexWithoutLock())
		rf.WakeupAllReplicators()
		mDebug(rf, "wakeup Start...")
	}  
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) AppendSingleLogWithoutLock(log LogEntry) {
	rf.log = append(rf.log, log)
	rf.persist()
	// mDebug(rf, "Append 1 log, len = %d, lastLogIndex = %d", len(rf.log), rf.lastLogIndex)
}

func (rf *Raft) CutLogListWithoutLock(index int) {
	// panic(index > len(rf.log))
	index = index - rf.firstLogIndex
	rf.log = rf.log[:index]
	rf.persist()
	mDebug(rf, "Cut log list, len = %d, lastLogIndex = %d", len(rf.log), rf.GetLastLogIndexWithoutLock())
}

func (rf *Raft) AppendLogListWithoutLock(logs []LogEntry, prevLogIndex int) {
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
	mDebug(rf, "Append %d logs, len = %d, lastLogIndex = %d", len(logs), rf.GetLastLogIndexWithoutLock())
}

func (rf *Raft) GetLastLogIndexAndTermWithoutLock() (int, int){
	lastLogIndex := rf.GetLastLogIndexWithoutLock()
	lastLog, _ := rf.GetLogAtIndexWithoutLock(lastLogIndex)
	return lastLogIndex, lastLog.Term
}

func (rf *Raft) GetLastLogTermWithoutLock() int {
	lastLogIndex := rf.GetLastLogIndexWithoutLock()
	lastLog, _ := rf.GetLogAtIndexWithoutLock(lastLogIndex)
	return lastLog.Term
}
func (rf *Raft) GetLastLogIndexWithoutLock() int {
	return len(rf.log) + rf.firstLogIndex - 1
}

func (rf *Raft) GetLogAtIndexWithoutLock(index int) (LogEntry, bool) {
	index = index - rf.firstLogIndex
	if index >= len(rf.log) {
		return LogEntry{}, false
	}
	return rf.log[index], true
}
func (rf *Raft) GetLogTermAtIndexWithoutLock(index int) int {
	log, ok := rf.GetLogAtIndexWithoutLock(index)
	if !ok {
		return -1
	}
	return log.Term
}