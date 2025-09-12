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
	mDebug(rf, "InstallSnapshot start")

	if !rf.CheckRPCTermWithoutLock(args.Term) {
		return
	}
	rf.lastHeartbeatTime = time.Now()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	rf.ApplySnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	mDebug(rf, "InstallSnapshot End")
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
	rf.snapshot = snapshot
	rf.snapshotEndIndex = index
	if rf.IsMatchPrevLog(index, term) {
		rf.log = rf.GetLogListBeginAtIndexWithoutLock(index)
		return
	}
	rf.log = []LogEntry{{Command: nil, Term: term}}
	if rf.lastApplied < index {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot: snapshot,
			SnapshotIndex: index,
			SnapshotTerm: term,
		}
		rf.lastApplied = index
		rf.commitIndex = max(rf.commitIndex, index)
	}
}