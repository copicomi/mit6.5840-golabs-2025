package raft

import "time"

type RPCType int
type RPCSendFunc func(int, interface{}, interface{}) bool
type RPCHandleFunc func(int, interface{}, interface{})
type RPCFactoryFunc func() interface{}

const (
	RPCRequestVote RPCType = iota
	RPCAppendEntries
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
	term := args.Term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.IsFoundAnotherLeader(term) {
		rf.ChangeRoleWithoutLock(Follower, term)
	}

	if term < rf.currentTerm ||
		rf.IsVotedForOthers(args.CandidateId) || 
		rf.IsNewerThan(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	rf.lastHeartbeatTime = time.Now()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := args.Term
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.IsFoundAnotherLeader(term) {
		rf.ChangeRoleWithoutLock(Follower, term)
	} 

	if term < rf.currentTerm {
		return
	} 

	rf.lastHeartbeatTime = time.Now()
	if !rf.IsMatchPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		return
	}

	rf.AppendLogListWithoutLock(args.Entries, args.PrevLogIndex)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
	}
	reply.Success = true
}

func (rf *Raft) HandleAppendReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO(3B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsFoundAnotherLeader(reply.Term) { 
		rf.ChangeRoleWithoutLock(Follower, reply.Term)
		return
	} 
	for !reply.Success {
		rf.nextIndex[server] --;
		args.PrevLogIndex = rf.nextIndex[server];
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		rf.sendAppendEntries(server, args, reply)
		// 直到 success 才会结束，否则一直重试
	}
	if reply.Success { 
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
	} 
}
func (rf *Raft) HandleVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.IsFoundAnotherLeader(reply.Term) { 
		rf.ChangeRoleWithoutLock(Follower, reply.Term)
		return
	} 
	if reply.VoteGranted {
		if rf.state != Candidate || rf.currentTerm != args.Term {
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


// example code to send a RequestVote RPC to a server.

// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { 
	if rf.lastLogIndex < rf.nextIndex[server] {
		// 不需要更新 log，欺骗上层节点发送成功
		reply.Term = rf.currentTerm
		reply.Success = true
		return true
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}