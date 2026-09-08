package raft

import "time"

func (n *Node) HandleClientPut(args *ClientPutArgs, reply *ClientPutReply) error {
	n.Lock()
	if n.state != Leader {
		reply.WhoLeader = n.whoLeader
		n.Unlock()
		return nil
	}

	entry := LogEntry{Term: n.currentTerm, Command: Command{Op: OpPut, Key: args.Key, Value: args.Value}}
	n.log = append(n.log, entry)
	targetIndex := len(n.log)
	term := n.currentTerm
	n.Unlock()

	for peerID, addr := range n.peers {
		go n.replicateToPeer(term, peerID, addr)
	}

	n.waitForCommit(targetIndex, term, &reply.Success, &reply.WhoLeader)

	return nil
}

func (n *Node) HandleClientGet(args *ClientGetArgs, reply *ClientGetReply) error {
	n.Lock()
	defer n.Unlock()
	if n.state != Leader {
		reply.WhoLeader = n.whoLeader
		return nil
	}

	val , ok := n.store[args.Key]
	reply.Value = val
	reply.Found = ok
	reply.Success = true
	reply.WhoLeader = n.whoLeader
	return nil
}

func (n *Node) waitForCommit(targetIndex int, term int, success *bool, whoLeader *string) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.Lock()
		if n.currentTerm != term || n.state != Leader {
			*whoLeader = n.whoLeader
			n.Unlock()
			return
		}

		if n.commitIndex >= targetIndex {
			n.Unlock()
			*success = true
			return
		}

		n.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
