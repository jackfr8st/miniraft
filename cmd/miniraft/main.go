package main

import (
	"flag"
	"log"
	"miniraft/raft"
	"strings"
)

func main() {

	id:= flag.String("id","","this node's id, e.g. node1")
	addr := flag.String("addr","","this node's listening address, e.g. 128.0.0.1:9001")
	peersFlag := flag.String("peers","","")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatal("must specify -id and -addr")
	}

	peers := parsePeers(*peersFlag)

	n := raft.NewNode(*id, peers)
	if err := n.Serve(*addr); err != nil {
		log.Fatal( err)
	}
	
	n.Run()

	select {} //blocked forever, since the node should continue to run till the process is killed

}


func parsePeers(s string) map[string]string {
	peers := map[string]string{}
	if s == ""{
		return peers
	}

	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2{
			log.Fatalf("invalid peer format: %s", pair)
		}
		peers[parts[0]] = parts[1]
	}
	return peers
}