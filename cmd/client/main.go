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

	addr := flag.String("addr","127.0.0.1:9001","a node address to try first")
	key := flag.String("key","","key")
	op := flag.String("op","put","put|get")
	value := flag.String("value","","value (for put)")
	flag.Parse()

	target := *addr
	for hops := 0; hops < 5; hops++ {
		conn, err := net.DialTimeout("tcp", target, time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error connecting to %s: %v", target, err)
			os.Exit(1)
		}

		client := rpc.NewClient(conn)

		if *op == "put" {
			args := &raft.ClientPutArgs{Key: *key, Value: *value}
			var reply raft.ClientPutReply
			client.Call("RaftRPC.ClientPut", args, &reply)
			client.Close()
			if !reply.Success && reply.WhoLeader != ""{
				fmt.Fprintf(os.Stderr, "(not leader, redirecting to: %s)\n", reply.WhoLeader)
				target = resolveHint(reply.WhoLeader)
				continue
			}
			if !reply.Success {
				fmt.Fprintf(os.Stderr, "put did not commit (timed out)")
				os.Exit(1)
			}
			fmt.Printf("OK put %s=%s (committed)\n", *key, *value)
			return
		}

		args := &raft.ClientGetArgs{Key: *key}
		var reply raft.ClientGetReply
		client.Call("RaftRPC.ClientGet", args, &reply)
		client.Close()

		if !reply.Success && reply.WhoLeader != ""{
			fmt.Fprintf(os.Stderr, "(not leader, redirecting to: %s)\n", reply.WhoLeader)
			target = resolveHint(reply.WhoLeader)
			continue
		}

		if !reply.Found{
			fmt.Println("(not found)")
			return
		}
		fmt.Println(reply.Value)
		return
		
	}
	fmt.Fprintf(os.Stderr, "too many redirects, giving up")
	os.Exit(1)
}



func resolveHint(id string) string {
	table := map[string]string{
		"node1": "127.0.0.1:9001",
		"node2": "127.0.0.1:9002",
		"node3": "127.0.0.1:9003",
	}
	if a, ok := table[id]; ok {
		return a
	}
	return id
}