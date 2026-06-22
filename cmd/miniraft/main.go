package main

import (
	"log"
	"miniraft/raft"
	"time"
)

func main() {

	peersFor := func(self string) map[string]string {
		all := map[string]string{
			"node1": "127.0.0.1:9001",
			"node2": "127.0.0.1:9002",
			"node3": "127.0.0.1:9003",
		}
		peers := map[string]string{}
		for id, addr := range all {
			if id != self {
				peers[id] = addr
			}
		}
	return peers
	}

	n1 := raft.NewNode("node1", peersFor("node1"))
	n2 := raft.NewNode("node2", peersFor("node2"))
	n3 := raft.NewNode("node3", peersFor("node3"))

	if err := n1.Serve(":9001"); err != nil {
		log.Fatal( err)
	}
	if err := n2.Serve(":9002"); err != nil {
		log.Fatal(err)
	}
	if err := n3.Serve(":9003"); err != nil {
		log.Fatal(err)
	}

	n1.Run()
	n2.Run()
	n3.Run()

	time.Sleep(3*time.Second)


}