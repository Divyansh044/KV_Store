package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

var httpClient = &http.Client{Timeout: rpcTimeout}

func (n *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// stale RPC from an old term — reject outright
	if req.Term < n.currentTerm {
		json.NewEncoder(w).Encode(RequestVoteResponse{Term: n.currentTerm, VoteGranted: false})
		return
	}

	// candidate's term is newer — step down, adopt it, clear our vote
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, "")
	}

	resp := RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}

	// grant vote only if we haven't voted this term (or already voted for this
	// candidate) AND the candidate's log is at least as up-to-date as ours
	// (Raft §5.4.1) — this is what prevents electing a leader missing committed entries.
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateID
	logOk := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if !alreadyVoted && logOk {
		resp.VoteGranted = true
		n.votedFor = req.CandidateID
		n.resetElectionDeadline()
	}

	json.NewEncoder(w).Encode(resp)
}

func (n *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm, Success: false}

	// reject if leader's term is behind ours
	if req.Term < n.currentTerm {
		json.NewEncoder(w).Encode(resp)
		return
	}

	// valid leader for this (or a newer) term — accept it, reset our timer
	n.becomeFollower(req.Term, req.LeaderID)
	n.resetElectionDeadline()
	resp.Term = n.currentTerm

	// log consistency check (§5.3): our log must contain an entry at
	// PrevLogIndex whose term matches PrevLogTerm, or we reject and the
	// leader will back off nextIndex and retry with an earlier point.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.lastLogIndex() || n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
			json.NewEncoder(w).Encode(resp)
			return
		}
	}

	// consistent — truncate any conflicting suffix, then append new entries.
	n.log = append(n.log[:req.PrevLogIndex], req.Entries...)

	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if last := n.lastLogIndex(); newCommit > last {
			newCommit = last
		}
		n.commitIndex = newCommit
		n.signalApply()
	}

	resp.Success = true
	json.NewEncoder(w).Encode(resp)
}

func (n *RaftNode) StartRaftServer(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/request-vote", n.handleRequestVote)
	mux.HandleFunc("/append-entries", n.handleAppendEntries)

	fmt.Printf("[%s] Raft server listening on %s\n", n.id, addr)
	return http.ListenAndServe(addr, mux)
}

func sendRequestVote(peerAddr string, req RequestVoteRequest) (*RequestVoteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/request-vote", peerAddr)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func sendAppendEntries(peerAddr string, req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/append-entries", peerAddr)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
