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
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	if rf.state != Leader {
		return index, term, isLeader
	}  

	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{
		Term: term,
		Command: command,
	})
	rf.lastLogIndex ++

	return index, term, isLeader
}

func (rf *Raft) IsNewerThan(lastLogIndex int, lastLogTerm int) bool {
	return false
	if rf.lastLogterm > lastLogTerm {
		return true
	} else if rf.lastLogterm == lastLogTerm && rf.lastLogIndex >= lastLogIndex {
		return true
	}
	return false
}