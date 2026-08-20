package main

import (
	"miniraft/raft"
	"time"
)

func main() {
	n := raft.NewNode("node1", map[string]string{})
	n.Run()
	time.Sleep(2 * time.Second) 
}