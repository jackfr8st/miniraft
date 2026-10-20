package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node1.state.json")
	n := &Node{
		statePath:   path,
		currentTerm: 7,
		votedFor:    "node2",
		log: []LogEntry{
			{Term: 1, Command: Command{Op: OpPut, Key: "a", Value: "1"}},
			{Term: 3, Command: Command{Op: OpDelete, Key: "a"}},
			{Term: 7, Command: Command{Op: OpPut, Key: "b", Value: "two words"}},
		},
	}
	if err := n.saveStateLocked(); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, found, err := loadState(path)
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	if got.CurrentTerm != n.currentTerm || got.VotedFor != n.votedFor {
		t.Errorf("got term=%d votedFor=%q, want term=%d votedFor=%q",
			got.CurrentTerm, got.VotedFor, n.currentTerm, n.votedFor)
	}
	if !reflect.DeepEqual(got.Log, n.log) {
		t.Errorf("log mismatch:\n got  %+v\n want %+v", got.Log, n.log)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, found, err := loadState(filepath.Join(t.TempDir(), "absent.state.json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("found=true for a missing file")
	}
}

func TestLoadCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.state.json")
	if err := os.WriteFile(path, []byte(`{"CurrentTerm": 3, "Log": [`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadState(path); err == nil {
		t.Fatal("expected an error for corrupt JSON")
	}
}

func TestRepeatedSavesLastWriteWinsNoTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node1.state.json")
	n := &Node{statePath: path}
	for i := 1; i <= 5; i++ {
		n.currentTerm = i
		n.votedFor = "node" + string(rune('0'+i))
		n.log = append(n.log, LogEntry{Term: i, Command: Command{Op: OpPut, Key: "k", Value: string(rune('a' + i))}})
		if err := n.saveStateLocked(); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file left behind (stat err=%v)", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("want only the state file in %s, got %v", dir, names)
	}

	got, found, err := loadState(path)
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	if got.CurrentTerm != 5 || got.VotedFor != "node5" || !reflect.DeepEqual(got.Log, n.log) {
		t.Errorf("last write did not win: got term=%d votedFor=%q logLen=%d", got.CurrentTerm, got.VotedFor, len(got.Log))
	}
}
