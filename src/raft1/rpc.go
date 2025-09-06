package raft

import "time"

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

	if term < rf.currentTerm {
		return
	} else if term > rf.currentTerm {
		rf.ChangeRoleWithoutLock(Follower, term)
	} else if rf.votedFor != Nobody && rf.votedFor != args.CandidateId {
		return
	} else {
		// TODO(3A): 检查 candidate 的 log 是否至少和自己一样新
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true

	term := args.Term
	if term < rf.currentTerm {
		reply.Success = false
		return
	} else if term > rf.currentTerm {
		rf.ChangeRoleWithoutLock(Follower, term)
	} else { // term == rf.currentTerm
		if rf.state == Candidate {
			rf.ChangeRoleWithoutLock(Follower, term)
		}
	}
	rf.lastHeartbeatTime = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { 
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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