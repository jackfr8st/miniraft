package raft

import (
	"log"
	"net"
	"net/rpc"
	"time"
)

type RaftRPC struct {
	node *Node
}

func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.node.HandleRequestVote(args, reply)
	return nil
}	

func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.node.HandleAppendEntries(args, reply)
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

func callRPC(addr, method string, args, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}


func (r *RaftRPC) ClientPut(args *ClientPutArgs, reply *ClientPutReply) error {
	return r.node.HandleClientPut(args, reply)
}

func (r *RaftRPC) ClientGet(args *ClientGetArgs, reply *ClientGetReply) error {
	return r.node.HandleClientGet(args, reply)
}

func (r *RaftRPC) DebugState(args *DebugStateArgs, reply *DebugStateReply) error {
	return r.node.HandleDebugState(args, reply)
}
