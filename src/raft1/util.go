package raft

import (
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

func GetRand(min int, max int) int {
	return rand.Intn(max-min) + min
}