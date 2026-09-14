package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"time"

	"miniraft/raft"
)

type nodeProc struct {
	id   string
	addr string
	args []string
	cmd  *exec.Cmd
}

func startNode(n *nodeProc) error {
	cmd := exec.Command(".\\bin\\miniraft.exe", n.args...)
	outFile, err := os.Create(n.id + ".log")
	if err != nil {
		return err
	}
	errFile, err := os.Create(n.id + ".err.log")
	if err != nil {
		return err
	}
	cmd.Stdout = outFile
	cmd.Stderr = errFile
	if err := cmd.Start(); err != nil {
		return err
	}
	n.cmd = cmd
	return nil
}

func isAlive(n *nodeProc) bool {
	if n.cmd == nil || n.cmd.Process == nil {
		return false
	}
	// A process that hasn't been Wait()'d and hasn't errored on signal 0
	// check is considered alive. Simplest reliable check: try FindProcess.
	proc, err := os.FindProcess(n.cmd.Process.Pid)
	if err != nil {
		return false
	}
	// On Windows, FindProcess always succeeds; check via Signal(syscall.Signal(0))
	// is POSIX-only, so instead we track exit explicitly via a goroutine (see main).
	_ = proc
	return true
}

func doPut(addr, key, value string) (success bool, whoLeader string, err error) {
	conn, dialErr := net.DialTimeout("tcp", addr, time.Second)
	if dialErr != nil {
		return false, "", dialErr
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	args := &raft.ClientPutArgs{Key: key, Value: value}
	var reply raft.ClientPutReply
	if callErr := client.Call("RaftRPC.ClientPut", args, &reply); callErr != nil {
		return false, "", callErr
	}
	return reply.Success, reply.WhoLeader, nil
}

func doGet(addr, key string) (found bool, value string, whoLeader string, success bool, err error) {
	conn, dialErr := net.DialTimeout("tcp", addr, time.Second)
	if dialErr != nil {
		return false, "", "", false, dialErr
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	args := &raft.ClientGetArgs{Key: key}
	var reply raft.ClientGetReply
	if callErr := client.Call("RaftRPC.ClientGet", args, &reply); callErr != nil {
		return false, "", "", false, callErr
	}
	return reply.Found, reply.Value, reply.WhoLeader, reply.Success, nil
}

// putWithRedirect follows "not leader" hints up to 5 hops.
func putWithRedirect(addrTable map[string]string, startAddr, key, value string) error {
	target := startAddr
	for hops := 0; hops < 5; hops++ {
		success, whoLeader, err := doPut(target, key, value)
		if err != nil {
			return err
		}
		if success {
			return nil
		}
		if whoLeader == "" {
			return fmt.Errorf("put failed, no leader hint")
		}
		target = addrTable[whoLeader]
	}
	return fmt.Errorf("too many redirects")
}

func getWithRedirect(addrTable map[string]string, startAddr, key string) (string, bool, error) {
	target := startAddr
	for hops := 0; hops < 5; hops++ {
		found, value, whoLeader, success, err := doGet(target, key)
		if err != nil {
			return "", false, err
		}
		if success {
			return value, found, nil
		}
		if whoLeader == "" {
			return "", false, fmt.Errorf("get failed, no leader hint")
		}
		target = addrTable[whoLeader]
	}
	return "", false, fmt.Errorf("too many redirects")
}

func main() {
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	killInterval := flag.Duration("killInterval", 5*time.Second, "how often to kill a random node")
	flag.Parse()

	os.Remove("node1.state.json")
	os.Remove("node2.state.json")
	os.Remove("node3.state.json")

	addrTable := map[string]string{
		"node1": "127.0.0.1:9001",
		"node2": "127.0.0.1:9002",
		"node3": "127.0.0.1:9003",
	}

	nodes := []*nodeProc{
		{id: "node1", addr: "127.0.0.1:9001", args: []string{"-id=node1", "-addr=:9001", "-peers=node2=127.0.0.1:9002,node3=127.0.0.1:9003"}},
		{id: "node2", addr: "127.0.0.1:9002", args: []string{"-id=node2", "-addr=:9002", "-peers=node1=127.0.0.1:9001,node3=127.0.0.1:9003"}},
		{id: "node3", addr: "127.0.0.1:9003", args: []string{"-id=node3", "-addr=:9003", "-peers=node1=127.0.0.1:9001,node2=127.0.0.1:9002"}},
	}

	for _, n := range nodes {
		if err := startNode(n); err != nil {
			log.Fatalf("failed to start %s: %v", n.id, err)
		}
	}
	fmt.Println("Started 3 nodes. Waiting 3s for initial election...")
	time.Sleep(3 * time.Second)

	written := map[string]string{}
	writeCounter := 0
	failures := 0
	deadline := time.Now().Add(*duration)
	nextKill := time.Now().Add(*killInterval)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for time.Now().Before(deadline) {
		writeCounter++
		key := fmt.Sprintf("k%d", writeCounter)
		val := fmt.Sprintf("v%d", writeCounter)

		if err := putWithRedirect(addrTable, "127.0.0.1:9001", key, val); err != nil {
			fmt.Printf("PUT %s=%s -> FAILED: %v\n", key, val, err)
		} else {
			written[key] = val
			fmt.Printf("PUT %s=%s -> OK\n", key, val)
		}

		// Verify a bounded sample.
		keys := make([]string, 0, len(written))
		for k := range written {
			keys = append(keys, k)
		}
		sampleSize := 5
		if len(keys) < sampleSize {
			sampleSize = len(keys)
		}
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		for _, k := range keys[:sampleSize] {
			expected := written[k]
			got, found, err := getWithRedirect(addrTable, "127.0.0.1:9001", k)
			if err != nil {
				fmt.Printf("!!! ERROR reading key=%s: %v\n", k, err)
				failures++
				continue
			}
			if !found || got != expected {
				fmt.Printf("!!! DATA LOSS/MISMATCH: key=%s expected=%s found=%v got=%s\n", k, expected, found, got)
				failures++
			}
		}

		if time.Now().After(nextKill) {
			alive := []int{}
			for i, n := range nodes {
				if n.cmd.ProcessState == nil { // hasn't exited yet
					alive = append(alive, i)
				}
			}
			if len(alive) > 0 {
				victim := nodes[alive[rng.Intn(len(alive))]]
				fmt.Printf(">>> Killing %s <<<\n", victim.id)
				victim.cmd.Process.Kill()
				victim.cmd.Wait() // reap, sets ProcessState
			}
			for _, n := range nodes {
				if n.cmd.ProcessState != nil {
					if err := startNode(n); err != nil {
						log.Printf("failed to restart %s: %v", n.id, err)
					} else {
						fmt.Printf(">>> Restarted %s <<<\n", n.id)
					}
				}
			}
			nextKill = time.Now().Add(*killInterval)
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("=== Chaos test complete ===")
	fmt.Printf("Writes attempted: %d, keys tracked: %d, failures: %d\n", writeCounter, len(written), failures)

	for _, n := range nodes {
		if n.cmd.ProcessState == nil {
			n.cmd.Process.Kill()
		}
	}

	if failures > 0 {
		fmt.Println("RESULT: FAIL - data loss or mismatch detected")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS - all committed writes survived")
}