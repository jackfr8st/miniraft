package raft

import (
	"encoding/json"
	"os"
)

type persistedState struct {
	CurrentTerm int
	VotedFor    string
	Log []LogEntry
}

func (n *Node) saveStateLocked() error {
	if n.statePath == ""{
		return nil
	}

	state := persistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := n.statePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Remove(n.statePath); err != nil && !os.IsNotExist(err){
		return err
	}
	return os.Rename(tmpPath, n.statePath) //either old file or fully-written new file will be present, never a half-written file
}



func loadState(path string)(persistedState, bool, error){
	data, err := os.ReadFile(path)
	if os.IsNotExist(err){
		return persistedState{}, false, nil
	}
	if err != nil {
		return persistedState{}, false, err
	}
	
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistedState{}, false, err
	}
	return state, true, nil
}