package raft

func (rf *Raft) incVoteCountWithoutLock() {
	rf.voteCount ++
}

func (rf *Raft) incTermWithoutLock() {
	rf.currentTerm ++
	rf.votedFor = Nobody
	rf.voteCount = 0
}
func (rf *Raft) incTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.incTermWithoutLock()
}

func (rf *Raft) ChangeRoleWithoutLock(role int, term int) { 
	rf.state = role
	rf.currentTerm = term
	if role == Follower {
		rf.votedFor = Nobody
	} else if role == Candidate {
		rf.votedFor = rf.me
		rf.voteCount = 1
	}
}

func (rf *Raft) ChangeRole(role int, term int) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ChangeRoleWithoutLock(role, term)
}