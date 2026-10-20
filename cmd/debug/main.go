package main

import (
	"flag"
	"fmt"
	"miniraft/raft"
	"net"
	"net/rpc"
	"os"
	"time"
)

func main() {

	addr := flag.String("addr", "127.0.0.1:9001", "address of the node to inspect")
	flag.Parse()

	conn, err := net.DialTimeout("tcp", *addr, time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to %s: %v\n", *addr, err)
		os.Exit(1)
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	var reply raft.DebugStateReply
	if err := client.Call("RaftRPC.DebugState", &raft.DebugStateArgs{}, &reply); err != nil {
		fmt.Fprintf(os.Stderr, "DebugState RPC to %s failed: %v\n", *addr, err)
		os.Exit(1)
	}
	fmt.Printf("%+v\n", reply)
}
