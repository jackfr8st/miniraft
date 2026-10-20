package raft

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	dialTimeout = 300 * time.Millisecond
	callTimeout = 500 * time.Millisecond
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

// One persistent connection per peer address, shared by every goroutine
// talking to that peer (rpc.Client is safe for concurrent use). Dialing per
// call left a TIME_WAIT socket behind each RPC, which under load exhausted
// Windows' ephemeral ports and caused election storms.
type connEntry struct {
	ready  chan struct{} // closed once the dial finishes
	client *rpc.Client
	err    error
}

var (
	connMu sync.Mutex
	conns  = map[string]*connEntry{}
)

// getConn returns the cached client for addr, dialing at most once even
// when several goroutines miss at the same time.
func getConn(addr string) (*connEntry, error) {
	connMu.Lock()
	e, ok := conns[addr]
	if !ok {
		e = &connEntry{ready: make(chan struct{})}
		conns[addr] = e
	}
	connMu.Unlock()

	if ok {
		<-e.ready
		return e, e.err
	}

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		e.err = err
		dropConn(addr, e) // let the next call redial
	} else {
		e.client = rpc.NewClient(conn)
	}
	close(e.ready)
	return e, e.err
}

// dropConn evicts e (only if it is still the cached entry, so a newer
// connection is never discarded) and closes its client.
func dropConn(addr string, e *connEntry) {
	connMu.Lock()
	if conns[addr] == e {
		delete(conns, addr)
	}
	connMu.Unlock()
	if e.client != nil {
		e.client.Close()
	}
}

func callRPC(addr, method string, args, reply interface{}) error {
	e, err := getConn(addr)
	if err != nil {
		return err
	}
	// net/rpc has no per-call deadline, so race the call against a timer.
	call := e.client.Go(method, args, reply, make(chan *rpc.Call, 1))
	timer := time.NewTimer(callTimeout)
	defer timer.Stop()
	select {
	case <-call.Done:
		err = call.Error
	case <-timer.C:
		err = errors.New("rpc timeout: " + method + " to " + addr)
	}
	if err != nil {
		dropConn(addr, e) // a dead or restarted peer gets a fresh dial next time
	}
	return err
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
