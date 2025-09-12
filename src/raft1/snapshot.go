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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) { 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	mDebugIndex(rf, "InstallSnapshot start")

	if !rf.CheckRPCTermWithoutLock(args.Term) {
		mDebugIndex(rf, "InstallSnapshot reject Term")
		return
	}
	rf.lastHeartbeatTime = time.Now()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		mDebugIndex(rf, "InstallSnapshot reject")
		return
	}

	rf.ApplySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	mDebugIndex(rf, "InstallSnapshot end")
	rf.persist()
}

func (rf *Raft) HandleInstallReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.CheckRPCTermWithoutLock(reply.Term) {
		return
	}
	rf.UpdateServerMatchIndex(server, args.LastIncludedIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) ApplySnapshot(snapshot []byte, index int, term int) {
	if index <= rf.snapshotEndIndex {
		return
	}
	if rf.IsMatchPrevLog(index, term) {
		rf.log = rf.GetLogListBeginAtIndexWithoutLock(index)
	} else {
		rf.log = []LogEntry{{Command: nil, Term: term}}
	}
	if rf.lastApplied < index {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot: snapshot,
			SnapshotIndex: index,
			SnapshotTerm: term,
		}
		rf.snapshot = snapshot
	}
	rf.snapshotEndIndex = index
	rf.lastApplied = max(rf.lastApplied, index)
	rf.commitIndex = max(rf.commitIndex, index)
}
func (rf *Raft) SnapShotWithLock(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SnapShotWithoutLock(index, snapshot)
}
func (rf *Raft) SnapShotWithoutLock(index int, snapshot []byte) {
	mDebug(rf, "SNAPSHOT at %d", index)
	mDebugIndex(rf, "snapshot")
	defer mDebugIndex(rf, "snapshot")
	if index <= rf.snapshotEndIndex || index > rf.commitIndex {
		mDebug(rf, "reject SNAPSHOT")
		return
	}
	rf.log = rf.GetLogListBeginAtIndexWithoutLock(index)
	rf.snapshotEndIndex = index
	rf.snapshot = snapshot
	rf.lastApplied = max(rf.lastApplied, index)
	rf.persist()
}