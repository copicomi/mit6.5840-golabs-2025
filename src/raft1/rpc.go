package raft

import "time"

type RPCType int
type RPCSendFunc func(int, interface{}, interface{}) bool
type RPCHandleFunc func(int, interface{}, interface{})
type RPCFactoryFunc func() interface{}

const (
	RPCRequestVote RPCType = iota
	RPCAppendEntries
	RPCInstallSnapshot
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

const (
	Nobody int = -1
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	 //mDebug(rf, "Got Vote RPC from %d, term %d", args.CandidateId, args.Term)
	if !rf.CheckRPCTermWithoutLock(args.Term) {
		return 
	}
	if rf.IsFoundAnotherCandidateInSameTerm(args.Term) {
		rf.ChangeRoleWithoutLock(Follower, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.IsVotedForOthers(args.CandidateId) || 
		rf.IsNewerThan(args.LastLogIndex, args.LastLogTerm) {
			//mDebug(rf, "Reject vote RPC")
		return
	}
	rf.lastHeartbeatTime = time.Now()

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
	 //mDebug(rf, "Grand vote to %d", args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		 mDebug(rf, "Got append RPC with pervLogIndex %d loglen %d term %d", args.PrevLogIndex, len(args.Entries), args.Term)
		 mDebugIndex(rf, "")
	}
	term := args.Term
	reply.Term = rf.currentTerm
	reply.Success = false
	if !rf.CheckRPCTermWithoutLock(term) {
		 mDebug(rf, "Reject append RPC, term %d, current %d", term, rf.currentTerm)
		return
	} 
	rf.lastHeartbeatTime = time.Now()
	if rf.IsExistedInSnapshot(args.PrevLogIndex + 1) {
		mDebug(rf, "Reject append RPC, log is in snapshot")
		return
	}
	if !rf.IsMatchPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		//mDebug(rf, "Reject append RPC, log is not match")
		return
	}
	rf.AppendLogListWithoutLock(args.Entries, args.PrevLogIndex)
		// mDebug(rf, "Accept append RPC, prevLogIndex %d, term %d, rf.lastLogIndex %d", args.PrevLogIndex, args.Term, rf.GetLastLogIndexWithoutLock())
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndexWithoutLock())
	}
	reply.Success = true
	rf.persist()
}

func (rf *Raft) HandleAppendReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.CheckRPCTermWithoutLock(reply.Term) {
		return
	}
	if reply.Success { 
		rf.UpdateServerMatchIndex(server, args.PrevLogIndex + len(args.Entries))
		// mDebug(rf, "[APP]Update matchIndex %d to %d", server, rf.matchIndex[server])
	} else {
		if rf.nextIndex[server] <= rf.snapshotEndIndex {
			mDebug(rf, "Reject handle append RPC without Backforwards")
			return
		}
		rf.BackforwardsNextIndex(server)
		// mDebug(rf, "Retry append with prevLogIndex %d", rf.nextIndex[server] - 1)
		rf.replicateCond[server].Signal()
	}
}
func (rf *Raft) HandleVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	 mDebug(rf, "Got Vote reply from %d, term %d", server, reply.Term)
	if !rf.CheckRPCTermWithoutLock(reply.Term) {
		return
	}
	if reply.VoteGranted {
		if rf.state != Candidate {
			return
		}
		rf.incVoteCountWithoutLock()
		if rf.voteCount > len(rf.peers) / 2 {
			// DPrintf("%d Winning Election", rf.me)
			rf.ChangeRoleWithoutLock(Leader, rf.currentTerm)
			go rf.SendHeartbeat()
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { 
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) CheckRPCTermWithoutLock(term int) bool {
	if rf.IsFoundAnotherLeader(term) {
		rf.ChangeRoleWithoutLock(Follower, term)
	}
	return !rf.IsOutofDate(term)
}