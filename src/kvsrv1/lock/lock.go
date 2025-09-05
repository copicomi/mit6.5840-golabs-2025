package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	LockKey string
	ClientID string
	// You may add code here
}

const (
	Unlocked = ""
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, LockKey : l, ClientID: kvtest.RandValue(8)}
	lk.ck.Put(lk.LockKey, Unlocked, 0)
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for true {
		val, ver, err := lk.ck.Get(lk.LockKey)
		if err == rpc.OK {
			if val == Unlocked {
				ok := lk.ck.Put(lk.LockKey, lk.ClientID, ver)
				if ok == rpc.OK {
					return
				} else if ok == rpc.ErrMaybe {
					newVal, _, checkErr := lk.ck.Get(lk.LockKey)
					if checkErr == rpc.OK && newVal == lk.ClientID{
						return
					}
				}
			} 
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for true {
		val, ver, err := lk.ck.Get(lk.LockKey)
		if err == rpc.OK {
			if val == lk.ClientID {
				ok := lk.ck.Put(lk.LockKey, Unlocked, ver)
				if ok == rpc.OK {
					return
				} else if ok == rpc.ErrMaybe {
					newVal, _, checkErr := lk.ck.Get(lk.LockKey)
					if checkErr == rpc.OK && newVal != lk.ClientID {
						return
					}
				}
			} 
		}
		time.Sleep(10 * time.Millisecond)
	}
}