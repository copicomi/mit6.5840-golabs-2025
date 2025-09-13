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
	rf.applyMu.Lock()
	defer rf.mu.Unlock()
	defer rf.applyMu.Unlock()
	mDebugIndex(rf, "InstallSnapshot start")

	reply.Term = rf.currentTerm
	if !rf.CheckRPCTermWithoutLock(args.Term) {
		mDebugIndex(rf, "InstallSnapshot reject Term")
		return
	}

	reply.Term = rf.currentTerm
	rf.lastHeartbeatTime = time.Now()

	if args.LastIncludedIndex <= rf.commitIndex {
		mDebugIndex(rf, "InstallSnapshot reject")
		return
	}

	rf.ApplySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	mDebugIndex(rf, "InstallSnapshot end")
}

func (rf *Raft) HandleInstallReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	mDebug(rf, "HandleInstallReply from server %d term %d", server, reply.Term)
	if !rf.CheckRPCTermWithoutLock(reply.Term) {
		return
	}
	if rf.state != Leader {
		return
	}
	rf.UpdateServerMatchIndex(server, args.LastIncludedIndex)
	rf.replicateCond[server].Signal()
	mDebug(rf, "InstallSnapshot update matchIndex %d to %d", server, args.LastIncludedIndex)
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
	rf.persist()
}
func (rf *Raft) SnapShotWithLock(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SnapShotWithoutLock(index, snapshot)
}
func (rf *Raft) SnapShotWithoutLock(index int, snapshot []byte) {
	mDebug(rf, "SNAPSHOT at %d", index)
	// mDebugIndex(rf, "snapshot")
	defer mDebugIndex(rf, "snapshot END")
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