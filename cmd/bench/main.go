// Command bench measures MiniRaft write throughput/latency under N concurrent
// clients and leader-failover time, and prints the results as markdown.
//
// Each concurrency level runs against a brand-new 3-node cluster in its own
// temp directory, so log growth from one level never skews the next (every
// put rewrites the full JSON log, so throughput degrades as the log grows).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"miniraft/raft"
)

var nodeIDs = []string{"node1", "node2", "node3"}

// ---------------------------------------------------------------------------
// Cluster management

type node struct {
	id   string
	addr string
	args []string
	cmd  *exec.Cmd
	done chan struct{} // closed when the process exits
}

type cluster struct {
	bin   string
	dir   string
	nodes []*node
	byID  map[string]*node
}

// liveClusters lets the interrupt handler kill every node we've started.
var (
	liveMu       sync.Mutex
	liveClusters = map[*cluster]bool{}
)

func newCluster(bin, dir string, basePort int) *cluster {
	c := &cluster{bin: bin, dir: dir, byID: map[string]*node{}}
	addrs := map[string]string{}
	for i, id := range nodeIDs {
		addrs[id] = "127.0.0.1:" + strconv.Itoa(basePort+i)
	}
	for _, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other+"="+addrs[other])
			}
		}
		n := &node{
			id:   id,
			addr: addrs[id],
			args: []string{"-id=" + id, "-addr=" + addrs[id], "-peers=" + strings.Join(peers, ",")},
		}
		c.nodes = append(c.nodes, n)
		c.byID[id] = n
	}
	return c
}

func (c *cluster) start(n *node) error {
	logFile, err := os.OpenFile(filepath.Join(c.dir, n.id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.bin, n.args...)
	cmd.Dir = c.dir // state files land in the cluster's private dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	n.cmd = cmd
	n.done = make(chan struct{})
	go func() {
		cmd.Wait()
		logFile.Close()
		close(n.done)
	}()
	return nil
}

func (c *cluster) startAll() error {
	liveMu.Lock()
	liveClusters[c] = true
	liveMu.Unlock()
	for _, n := range c.nodes {
		if err := c.start(n); err != nil {
			c.stopAll()
			return fmt.Errorf("start %s: %w", n.id, err)
		}
	}
	return nil
}

func (c *cluster) kill(n *node) {
	if n.cmd == nil || !n.alive() {
		return
	}
	n.cmd.Process.Kill()
	<-n.done
}

func (c *cluster) stopAll() {
	for _, n := range c.nodes {
		c.kill(n)
	}
	liveMu.Lock()
	delete(liveClusters, c)
	liveMu.Unlock()
}

func (n *node) alive() bool {
	if n.done == nil {
		return false
	}
	select {
	case <-n.done:
		return false
	default:
		return true
	}
}

func (c *cluster) aliveNodes() []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.alive() {
			out = append(out, n)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// RPC helpers

// callTimeout wraps net/rpc, which has no per-call deadline of its own.
func callTimeout(client *rpc.Client, method string, args, reply any, timeout time.Duration) error {
	call := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		return call.Error
	case <-time.After(timeout):
		return errors.New("rpc timeout")
	}
}

func dial(addr string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func debugState(addr string) (raft.DebugStateReply, error) {
	var reply raft.DebugStateReply
	client, err := dial(addr, 100*time.Millisecond)
	if err != nil {
		return reply, err
	}
	defer client.Close()
	err = callTimeout(client, "RaftRPC.DebugState", &raft.DebugStateArgs{}, &reply, 500*time.Millisecond)
	return reply, err
}

// findLeader returns the live node reporting itself leader at the highest term.
func (c *cluster) findLeader() (*node, raft.DebugStateReply, bool) {
	var best *node
	var bestState raft.DebugStateReply
	for _, n := range c.aliveNodes() {
		st, err := debugState(n.addr)
		if err == nil && st.State == "Leader" && (best == nil || st.Term > bestState.Term) {
			best, bestState = n, st
		}
	}
	return best, bestState, best != nil
}

// monitor holds one DebugState connection per node, opened before load starts,
// so end-of-run stats are still readable if the OS has run out of ports.
type monitor map[*node]*rpc.Client

func (c *cluster) openMonitor() monitor {
	m := monitor{}
	for _, n := range c.nodes {
		if cl, err := dial(n.addr, 500*time.Millisecond); err == nil {
			m[n] = cl
		}
	}
	return m
}

func (m monitor) close() {
	for _, cl := range m {
		cl.Close()
	}
}

// snapshot returns the highest term seen and the log length of the
// highest-term leader; ok is false unless every node answered.
func (m monitor) snapshot(c *cluster) (term, leaderLogLen int, ok bool) {
	ok = len(m) == len(c.nodes)
	leaderTerm := -1
	for _, cl := range m {
		var st raft.DebugStateReply
		if err := callTimeout(cl, "RaftRPC.DebugState", &raft.DebugStateArgs{}, &st, time.Second); err != nil {
			ok = false
			continue
		}
		term = max(term, st.Term)
		if st.State == "Leader" && st.Term > leaderTerm {
			leaderTerm, leaderLogLen = st.Term, st.LogLen
		}
	}
	return term, leaderLogLen, ok
}

// waitStable waits for a leader whose commit index every live node has applied.
func (c *cluster) waitStable(timeout time.Duration) (*node, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader, ls, ok := c.findLeader(); ok {
			caughtUp := true
			for _, n := range c.aliveNodes() {
				st, err := debugState(n.addr)
				if err != nil || st.Term != ls.Term || st.LastApplied < ls.CommitIndex {
					caughtUp = false
					break
				}
			}
			if caughtUp {
				return leader, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("cluster not stable after %v", timeout)
}

// ---------------------------------------------------------------------------
// Throughput

type throughputResult struct {
	clients   int
	ops       int
	errors    int
	elapsed   time.Duration
	latencies []time.Duration // sorted
	logLen    int
	elections int // term increase during the run; 0 means one leader throughout
}

func (r throughputResult) opsPerSec() float64 { return float64(r.ops) / r.elapsed.Seconds() }

func (r throughputResult) pct(p float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	idx := int(float64(len(r.latencies))*p/100+0.5) - 1
	return r.latencies[max(0, min(idx, len(r.latencies)-1))]
}

// worker issues puts back-to-back to the leader, following "not leader" hints.
// It keeps one persistent connection per node so redirects never dial (fresh
// dials pile up TIME_WAIT sockets, which on Windows exhausts ephemeral ports).
// Only successful puts that complete before the deadline count.
func worker(c *cluster, id int, valueSize int, deadline time.Time) (lats []time.Duration, errCount int) {
	value := strings.Repeat("x", valueSize)
	target := c.nodes[id%len(c.nodes)]
	conns := map[*node]*rpc.Client{}
	defer func() {
		for _, cl := range conns {
			cl.Close()
		}
	}()
	dropAndRotate := func() {
		if cl := conns[target]; cl != nil {
			cl.Close()
			delete(conns, target)
		}
		target = c.nodes[(slices.Index(c.nodes, target)+1)%len(c.nodes)]
	}

	redirects := 0
	for i := 0; time.Now().Before(deadline); i++ {
		client := conns[target]
		if client == nil {
			var err error
			if client, err = dial(target.addr, 500*time.Millisecond); err != nil {
				errCount++
				dropAndRotate()
				time.Sleep(10 * time.Millisecond)
				continue
			}
			conns[target] = client
		}
		args := &raft.ClientPutArgs{Key: fmt.Sprintf("c%d-k%d", id, i%1000), Value: value}
		var reply raft.ClientPutReply
		start := time.Now()
		err := callTimeout(client, "RaftRPC.ClientPut", args, &reply, 5*time.Second)
		end := time.Now()
		switch {
		case err != nil:
			errCount++
			dropAndRotate()
		case reply.Success:
			redirects = 0
			if !end.After(deadline) {
				lats = append(lats, end.Sub(start))
			}
		case reply.WhoLeader != "" && reply.WhoLeader != target.id && c.byID[reply.WhoLeader] != nil:
			target = c.byID[reply.WhoLeader]
			if redirects++; redirects > 2 {
				// Bouncing between nodes: stale hints during an election.
				errCount++
				time.Sleep(10 * time.Millisecond)
			}
		default:
			// Leader timed out waiting for commit, or no leader known yet.
			errCount++
			time.Sleep(10 * time.Millisecond)
		}
	}
	return lats, errCount
}

func runThroughput(bin, root string, basePort, clients, valueSize int, duration time.Duration) (throughputResult, error) {
	dir := filepath.Join(root, fmt.Sprintf("throughput-c%d", clients))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return throughputResult{}, err
	}
	c := newCluster(bin, dir, basePort)
	if err := c.startAll(); err != nil {
		return throughputResult{}, err
	}
	defer c.stopAll()
	if _, err := c.waitStable(10 * time.Second); err != nil {
		return throughputResult{}, err
	}
	mon := c.openMonitor()
	defer mon.close()
	startTerm, _, _ := mon.snapshot(c)

	start := time.Now()
	deadline := start.Add(duration)
	var mu sync.Mutex
	var wg sync.WaitGroup
	res := throughputResult{clients: clients, elapsed: duration}
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lats, errCount := worker(c, i, valueSize, deadline)
			mu.Lock()
			res.latencies = append(res.latencies, lats...)
			res.errors += errCount
			mu.Unlock()
		}()
	}
	wg.Wait()

	slices.Sort(res.latencies)
	res.ops = len(res.latencies)
	endTerm, logLen, ok := mon.snapshot(c)
	res.elections, res.logLen = endTerm-startTerm, logLen
	if !ok {
		res.elections = -1 // unknown; rendered as "?"
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Failover

type failoverResult struct {
	trial     int
	killed    string
	newLeader string
	elected   time.Duration // kill -> a survivor reports Leader at a higher term
	firstPut  time.Duration // kill -> first successful put
	err       error
}

// probePut tries one put against addr, returning success and the leader hint.
func probePut(addr, key string) (bool, string) {
	client, err := dial(addr, 100*time.Millisecond)
	if err != nil {
		return false, ""
	}
	defer client.Close()
	var reply raft.ClientPutReply
	if err := callTimeout(client, "RaftRPC.ClientPut", &raft.ClientPutArgs{Key: key, Value: "v"}, &reply, 3*time.Second); err != nil {
		return false, ""
	}
	return reply.Success, reply.WhoLeader
}

func failoverTrial(c *cluster, trial int, timeout time.Duration) failoverResult {
	res := failoverResult{trial: trial}
	if _, err := c.waitStable(10 * time.Second); err != nil {
		res.err = err
		return res
	}
	leader, ls, ok := c.findLeader()
	if !ok {
		res.err = errors.New("no leader before kill")
		return res
	}
	res.killed = leader.id
	var survivors []*node
	for _, n := range c.nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}

	t0 := time.Now()
	c.kill(leader)
	deadline := t0.Add(timeout)

	// Watch for the election independently of the write probe.
	electedCh := make(chan time.Duration, 1)
	stopWatch := make(chan struct{})
	go func() {
		for time.Now().Before(deadline) {
			for _, n := range survivors {
				if st, err := debugState(n.addr); err == nil && st.State == "Leader" && st.Term > ls.Term {
					electedCh <- time.Since(t0)
					return
				}
			}
			select {
			case <-stopWatch:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	defer close(stopWatch)

	key := fmt.Sprintf("failover-%d", trial)
	target := survivors[0]
	for i := 0; time.Now().Before(deadline); i++ {
		success, hint := probePut(target.addr, key)
		if success {
			res.firstPut = time.Since(t0)
			res.newLeader = target.id
			break
		}
		if h := c.byID[hint]; h != nil && h != leader && h != target {
			target = h // follow a hint that points at a live node
		} else {
			target = survivors[(i+1)%len(survivors)]
			time.Sleep(2 * time.Millisecond)
		}
	}
	if res.firstPut == 0 {
		res.err = fmt.Errorf("no successful write within %v", timeout)
	}
	select {
	case res.elected = <-electedCh:
	case <-time.After(500 * time.Millisecond):
	}

	if err := c.start(leader); err != nil {
		res.err = errors.Join(res.err, fmt.Errorf("restart %s: %w", leader.id, err))
	}
	return res
}

func runFailover(bin, root string, basePort, trials int, timeout time.Duration) ([]failoverResult, error) {
	dir := filepath.Join(root, "failover")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := newCluster(bin, dir, basePort)
	if err := c.startAll(); err != nil {
		return nil, err
	}
	defer c.stopAll()

	var results []failoverResult
	for t := 1; t <= trials; t++ {
		r := failoverTrial(c, t, timeout)
		logf("  trial %d: killed=%s newLeader=%s elected=%s firstWrite=%s err=%v",
			t, r.killed, r.newLeader, fmtMS(r.elected), fmtMS(r.firstPut), r.err)
		results = append(results, r)
		time.Sleep(500 * time.Millisecond) // let the restarted node rejoin
	}
	return results, nil
}

// ---------------------------------------------------------------------------
// Reporting

func fmtMS(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000)
}

func renderMarkdown(w io.Writer, duration time.Duration, valueSize int, tp []throughputResult, fo []failoverResult) {
	fmt.Fprintf(w, "# MiniRaft benchmark\n\n")
	fmt.Fprintf(w, "3-node cluster on localhost · %s/%s · %d CPUs · %s · %s\n\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), time.Now().Format("2006-01-02 15:04"))

	if len(tp) > 0 {
		fmt.Fprintf(w, "## Write throughput & latency\n\n")
		fmt.Fprintf(w, "Each row: fresh cluster, closed-loop `put` clients for %s, %d-byte values. "+
			"Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). "+
			"*Elections* = term increase during the run (0 = one stable leader throughout).\n\n", duration, valueSize)
		fmt.Fprintln(w, "| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |")
		fmt.Fprintln(w, "|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
		for _, r := range tp {
			maxLat := time.Duration(0)
			if len(r.latencies) > 0 {
				maxLat = r.latencies[len(r.latencies)-1]
			}
			elections := "?"
			if r.elections >= 0 {
				elections = strconv.Itoa(r.elections)
			}
			logLen := "no leader"
			if r.logLen > 0 {
				logLen = strconv.Itoa(r.logLen)
			}
			fmt.Fprintf(w, "| %d | %d | %.1f | %s | %s | %s | %s | %d | %s | %s |\n",
				r.clients, r.ops, r.opsPerSec(), fmtMS(r.pct(50)), fmtMS(r.pct(95)), fmtMS(r.pct(99)),
				fmtMS(maxLat), r.errors, elections, logLen)
		}
		fmt.Fprintln(w)
	}

	if len(fo) > 0 {
		fmt.Fprintf(w, "## Leader failover\n\n")
		fmt.Fprintf(w, "Leader killed with an abrupt process kill (no graceful shutdown). "+
			"*Elected* = a survivor reports `Leader` at a higher term; *First write* = first `put` that commits. "+
			"Both are measured from the kill.\n\n")
		fmt.Fprintln(w, "| Trial | Killed | New leader | Elected | First write |")
		fmt.Fprintln(w, "|--:|:--|:--|--:|--:|")
		var elected, firstPut []time.Duration
		for _, r := range fo {
			newLeader := r.newLeader
			if r.err != nil {
				newLeader = "FAILED: " + r.err.Error()
			}
			fmt.Fprintf(w, "| %d | %s | %s | %s | %s |\n", r.trial, r.killed, newLeader, fmtMS(r.elected), fmtMS(r.firstPut))
			if r.err == nil {
				firstPut = append(firstPut, r.firstPut)
				if r.elected > 0 {
					elected = append(elected, r.elected)
				}
			}
		}
		for _, s := range []struct {
			name string
			f    func([]time.Duration) time.Duration
		}{{"min", sliceMin}, {"median", median}, {"max", sliceMax}} {
			fmt.Fprintf(w, "| **%s** | | | %s | %s |\n", s.name, fmtMS(s.f(elected)), fmtMS(s.f(firstPut)))
		}
		fmt.Fprintln(w)
	}
}

func median(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Sorted(slices.Values(d))
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func sliceMin(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return slices.Min(d)
}

func sliceMax(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return slices.Max(d)
}

func logf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

// ---------------------------------------------------------------------------

func buildNode(dir string) (string, error) {
	bin := filepath.Join(dir, "miniraft")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/miniraft")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return bin, cmd.Run()
}

func main() {
	duration := flag.Duration("duration", 30*time.Second, "how long to run each concurrency level")
	clientsFlag := flag.String("clients", "1,8,32", "comma-separated concurrent-client counts")
	valueSize := flag.Int("valueSize", 16, "bytes per put value")
	trials := flag.Int("failoverTrials", 5, "number of leader kills (0 to skip)")
	failoverTimeout := flag.Duration("failoverTimeout", 10*time.Second, "give up on a failover trial after this long")
	basePort := flag.Int("basePort", 9101, "nodes listen on basePort..basePort+2 (default avoids a dev cluster on 9001)")
	binFlag := flag.String("bin", "", "miniraft binary to use (default: build ./cmd/miniraft into the work dir)")
	outFlag := flag.String("out", "", "also write the markdown report to this file")
	keep := flag.Bool("keep", false, "keep the work dir (node logs, state files) after the run")
	flag.Parse()

	var clientCounts []int
	for _, s := range strings.Split(*clientsFlag, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			log.Fatalf("bad -clients value %q", s)
		}
		clientCounts = append(clientCounts, n)
	}

	root, err := os.MkdirTemp("", "miniraft-bench-*")
	if err != nil {
		log.Fatal(err)
	}
	cleanup := func() {
		liveMu.Lock()
		cs := make([]*cluster, 0, len(liveClusters))
		for c := range liveClusters {
			cs = append(cs, c)
		}
		liveMu.Unlock()
		for _, c := range cs {
			c.stopAll()
		}
		if *keep {
			logf("work dir kept at %s", root)
		} else {
			os.RemoveAll(root)
		}
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		logf("interrupted, stopping nodes")
		cleanup()
		os.Exit(130)
	}()
	fatal := func(format string, args ...any) {
		logf(format, args...)
		cleanup()
		os.Exit(1)
	}

	bin := *binFlag
	if bin == "" {
		logf("building ./cmd/miniraft ...")
		if bin, err = buildNode(root); err != nil {
			fatal("build failed: %v", err)
		}
	} else if bin, err = filepath.Abs(bin); err != nil {
		fatal("%v", err)
	}

	// Failover runs first: heavy throughput runs can leave the OS short of
	// ephemeral ports for a few minutes, which would skew elections.
	var fo []failoverResult
	if *trials > 0 {
		logf("failover: %d trial(s) on a fresh cluster ...", *trials)
		if fo, err = runFailover(bin, root, *basePort, *trials, *failoverTimeout); err != nil {
			fatal("failover: %v", err)
		}
	}

	var tp []throughputResult
	for _, n := range clientCounts {
		logf("throughput: %d client(s) for %s on a fresh cluster ...", n, *duration)
		r, err := runThroughput(bin, root, *basePort, n, *valueSize, *duration)
		if err != nil {
			fatal("throughput run with %d clients: %v", n, err)
		}
		logf("  %.1f ops/sec, p50=%s p99=%s, errors=%d", r.opsPerSec(), fmtMS(r.pct(50)), fmtMS(r.pct(99)), r.errors)
		tp = append(tp, r)
	}

	var sb strings.Builder
	renderMarkdown(&sb, *duration, *valueSize, tp, fo)
	fmt.Print(sb.String())
	if *outFlag != "" {
		if err := os.WriteFile(*outFlag, []byte(sb.String()), 0o644); err != nil {
			fatal("write %s: %v", *outFlag, err)
		}
	}
	cleanup()
}
