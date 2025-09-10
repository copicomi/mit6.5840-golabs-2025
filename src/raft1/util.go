package raft

import (
	"fmt"
	"log"
	"math/rand"
)

// Debugging
const Debug = false

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

func GetRand(min int, max int) int {
	return rand.Intn(max-min) + min
}