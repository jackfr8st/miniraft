package raft

import (
	"log"
	"net"
	"net/rpc"
)

type RaftRPC struct {
	node *Node
}

func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.node.HandleRequestVote(args, reply)
	return nil
}	

func (n *Node) Serve(addr string) error {
	server := rpc.NewServer()
	if err := server.RegisterName("RaftRPC", &RaftRPC{node: n}); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Printf("[%s] listening on %s", n.id, addr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return 
			}
		go server.ServeConn(conn)
		}
	}()
	return nil
}
