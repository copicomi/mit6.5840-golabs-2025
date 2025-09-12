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
	rf.SnapShotWithoutLock(args.LastIncludedIndex, args.Data)
	rf.ApplySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	mDebug(rf, "InstallSnapshot End")
}

func (rf *Raft) HandleInstallReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
}

func (rf *Raft) ApplySnapshot(snapshot []byte, index int, term int) {
	rf.applyCh <- raftapi.ApplyMsg{
		Snapshot:      snapshot,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		SnapshotValid: true,
	}
}