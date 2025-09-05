package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVEntry struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	Mu sync.Mutex
	Data map[string]KVEntry;
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{Data: make(map[string]KVEntry)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	kv.Mu.Lock()
	defer kv.Mu.Unlock()
	entry, ok := kv.Data[key]
	if ok {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	key := args.Key
	value := args.Value
	version := args.Version
	
	kv.Mu.Lock()
	defer kv.Mu.Unlock()
	entry, ok := kv.Data[key]
	if ok {
		if version == entry.Version {
			kv.Data[key] = KVEntry{
				Value: value,
				Version: entry.Version + 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if version == 0 {
			kv.Data[key] = KVEntry{
				Value: value,
				Version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}

	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
