package raft

import (
	"time"

	"6.5840/raftapi"
)

type InstallSnapshotArgs struct {
    Term int
    LeaderId int
    LastIncludedIndex int
	LastIncludedTerm int
    Data []byte
}

type InstallSnapshotReply struct {
    Term int
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	mDebug(rf, "InstallSnapshot start")

	term := args.Term
	reply.Term = rf.currentTerm
	reply.Success = false
	if term < rf.currentTerm {
		return
	}
	if rf.IsFoundAnotherLeader(term) {
		rf.ChangeRoleWithoutLock(Follower, term)
	}
	rf.lastHeartbeatTime = time.Now()
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.ApplySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.SnapShotWithoutLock(args.LastIncludedIndex, args.Data)
	reply.Success = true
	mDebug(rf, "InstallSnapshot End")
}

func (rf *Raft) HandleInstallReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.currentTerm || rf.state != Leader {
		return
	}
	if rf.IsFoundAnotherLeader(reply.Term) { 
		rf.ChangeRoleWithoutLock(Follower, reply.Term)
		return
	} 
	if reply.Success { 
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex + 1)
		mDebug(rf, "[INS]Update matchIndex %d to %d", server, rf.matchIndex[server])
	} else {

		mDebug(rf, "[INS]Not Update matchIndex %d to %d", server, rf.matchIndex[server])
	}
}

func (rf *Raft) ApplySnapshot(snapshot []byte, index int, term int) {
	rf.applyCh <- raftapi.ApplyMsg{
		Snapshot:      snapshot,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		SnapshotValid: true,
	}
}