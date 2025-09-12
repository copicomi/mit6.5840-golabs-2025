package raft

import (
	"fmt"
	"log"
	"math/rand"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func mDebug(rf *Raft, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] S%d ", rf.currentTerm, rf.me)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func mDebugLock(rf *Raft, funcName string)  {
	mDebug(rf, "%s() Have lock", funcName)
}

func mDebugLogs(rf *Raft, msg string) {
	mDebug(rf, "%s, snapshotIndex %d, loglength %d, total %d", msg, rf.snapshotEndIndex, len(rf.log), rf.snapshotEndIndex + len(rf.log))
}

func mDebugIndex(rf *Raft, msg string)  {
	indexMsg := fmt.Sprintf("apply %d, commit %d, log %d, snapshot %d, len %d", rf.lastApplied, rf.commitIndex, rf.GetLastLogIndexWithoutLock(), rf.snapshotEndIndex, len(rf.log))
	mDebug(rf, "%s, %s", msg, indexMsg)
}

func GetRand(min int, max int) int {
	return rand.Intn(max-min) + min
}