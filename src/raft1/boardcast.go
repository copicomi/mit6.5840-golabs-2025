package raft

func (rf *Raft) Boardcast(
	argsFactory RPCFactoryFunc,
	replyFactory RPCFactoryFunc,
	sendFunction RPCSendFunc,
	handleFunction RPCHandleFunc,
) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := argsFactory()
			reply := replyFactory()
			sendFunction(server, args, reply)
			if (handleFunction != nil) {
				handleFunction(server, args, reply)
			}
		}(i)
	}
}