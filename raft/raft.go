package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one command in the replicated log.
// Position in the log slice IS the Raft index (1-based: log[0] is index 1).
type LogEntry struct {
	Term    int
	Command string
}

var ErrNotLeader = errors.New("not the leader")
var ErrCommitTimeout = errors.New("timed out waiting for command to commit")

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	rpcTimeout         = 200 * time.Millisecond
	proposeTimeout     = 3 * time.Second
)

type RaftNode struct {
	id    string
	peers map[string]string // peerId -> raft HTTP addr (does not include self)

	mu          sync.Mutex
	currentTerm int
	votedFor    string
	log         []LogEntry
	state       NodeState
	leaderId    string

	commitIndex int // highest log index known to be committed
	lastApplied int // highest log index applied to the state machine

	nextIndex  map[string]int // leader only: next log index to send each peer
	matchIndex map[string]int // leader only: highest log index known replicated on each peer

	electionDeadline time.Time
	applyFn          func(command string) // wired to the KV store by main.go

	applyTrigger chan struct{}
}

func NewRaftNode(id string, peers map[string]string) *RaftNode {
	return &RaftNode{
		id:           id,
		peers:        peers,
		currentTerm:  0,
		votedFor:     "",
		state:        Follower,
		log:          []LogEntry{},
		nextIndex:    make(map[string]int),
		matchIndex:   make(map[string]int),
		applyTrigger: make(chan struct{}, 1),
	}
}

// SetApplyFunc wires the callback invoked once per committed log entry, in order,
// on every node (leader included). main.go points this at the KVStore.
func (n *RaftNode) SetApplyFunc(fn func(command string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyFn = fn
}

func (n *RaftNode) ID() string { return n.id }

func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

func (n *RaftNode) Status() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return fmt.Sprintf("id=%s state=%s term=%d leader=%s logLen=%d commitIndex=%d",
		n.id, n.state, n.currentTerm, n.leaderId, len(n.log), n.commitIndex)
}

// ParsePeers parses "id1=host:port,id2=host:port" into a map.
func ParsePeers(csv string) (map[string]string, error) {
	peers := make(map[string]string)
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return peers, nil
	}
	for _, entry := range strings.Split(csv, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer entry %q, expected id=host:port", entry)
		}
		peers[parts[0]] = parts[1]
	}
	return peers, nil
}

// ============================================================
// Log helpers — caller must hold n.mu
// ============================================================

// lastLogIndex returns the 1-based index of the last log entry, or 0 if empty.
func (n *RaftNode) lastLogIndex() int {
	return len(n.log)
}

func (n *RaftNode) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// termAt returns the term of the entry at 1-based index, or 0 if out of range.
func (n *RaftNode) termAt(index int) int {
	if index <= 0 || index > len(n.log) {
		return 0
	}
	return n.log[index-1].Term
}

// ============================================================
// State transitions — caller must hold n.mu
// ============================================================

func (n *RaftNode) becomeFollower(term int, leaderId string) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderId = leaderId
}

func (n *RaftNode) resetElectionDeadline() {
	n.electionDeadline = time.Now().Add(randomElectionTimeout())
}

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// ============================================================
// Run — starts the background goroutines. Call once after construction.
// ============================================================

func (n *RaftNode) Run() {
	n.mu.Lock()
	n.resetElectionDeadline()
	n.mu.Unlock()

	go n.electionLoop()
	go n.applyLoop()
}

func (n *RaftNode) electionLoop() {
	const tick = 20 * time.Millisecond
	for {
		time.Sleep(tick)

		n.mu.Lock()
		state := n.state
		expired := time.Now().After(n.electionDeadline)
		n.mu.Unlock()

		if state == Leader {
			n.broadcastAppendEntries()
			time.Sleep(heartbeatInterval - tick)
			continue
		}

		if expired {
			n.startElection()
		}
	}
}

// ============================================================
// Elections
// ============================================================

func (n *RaftNode) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderId = ""
	term := n.currentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	n.resetElectionDeadline()
	peers := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	fmt.Printf("[%s] starting election for term %d\n", n.id, term)

	votes := 1 // vote for self
	totalNodes := len(peers) + 1
	votesNeeded := totalNodes/2 + 1
	var voteMu sync.Mutex
	done := make(chan struct{})
	var once sync.Once

	for peerId, addr := range peers {
		go func(peerId, addr string) {
			req := RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			resp, err := sendRequestVote(addr, req)
			if err != nil || resp == nil {
				return
			}

			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term, "")
				n.mu.Unlock()
				return
			}
			stillCandidate := n.state == Candidate && n.currentTerm == term
			n.mu.Unlock()

			if !stillCandidate || !resp.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
			gotMajority := votes >= votesNeeded
			voteMu.Unlock()

			if gotMajority {
				once.Do(func() { close(done) })
			}
		}(peerId, addr)
	}

	select {
	case <-done:
		n.becomeLeaderIfStillCandidate(term)
	case <-time.After(electionTimeoutMax):
		// election timed out without a majority — electionLoop will retry
	}
}

func (n *RaftNode) becomeLeaderIfStillCandidate(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Candidate || n.currentTerm != term {
		return // stale — term moved on or we already stepped down
	}
	n.state = Leader
	n.leaderId = n.id
	for peerId := range n.peers {
		n.nextIndex[peerId] = n.lastLogIndex() + 1
		n.matchIndex[peerId] = 0
	}
	fmt.Printf("[%s] elected leader for term %d\n", n.id, term)
}

// ============================================================
// Log replication (leader side)
// ============================================================

func (n *RaftNode) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	for peerId, addr := range peers {
		wg.Add(1)
		go func(peerId, addr string) {
			defer wg.Done()
			n.replicateToPeer(peerId, addr, term)
		}(peerId, addr)
	}
	wg.Wait()
	n.advanceCommitIndex()
}

func (n *RaftNode) replicateToPeer(peerId, addr string, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peerId]
	if nextIdx < 1 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAt(prevLogIndex)
	var entries []LogEntry
	if nextIdx <= len(n.log) {
		entries = append(entries, n.log[nextIdx-1:]...)
	}
	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := sendAppendEntries(addr, req)
	if err != nil || resp == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "")
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		n.matchIndex[peerId] = prevLogIndex + len(entries)
		n.nextIndex[peerId] = n.matchIndex[peerId] + 1
	} else if n.nextIndex[peerId] > 1 {
		n.nextIndex[peerId]--
	}
}

// advanceCommitIndex — leader only. A log index N is committed once it's on a
// majority of nodes AND it was written during the leader's *current* term
// (Raft §5.4.2 safety rule — never commit a prior-term entry by count alone).
func (n *RaftNode) advanceCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return
	}

	for N := n.lastLogIndex(); N > n.commitIndex; N-- {
		if n.termAt(N) != n.currentTerm {
			continue
		}
		count := 1 // leader itself has it
		for peerId := range n.peers {
			if n.matchIndex[peerId] >= N {
				count++
			}
		}
		totalNodes := len(n.peers) + 1
		if count >= totalNodes/2+1 {
			n.commitIndex = N
			n.signalApply()
			break
		}
	}
}

func (n *RaftNode) signalApply() {
	select {
	case n.applyTrigger <- struct{}{}:
	default:
	}
}

// ============================================================
// Apply loop — runs on every node, applies committed entries in order.
// ============================================================

func (n *RaftNode) applyLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.applyTrigger:
		case <-ticker.C:
		}
		n.applyCommitted()
	}
}

func (n *RaftNode) applyCommitted() {
	n.mu.Lock()
	fn := n.applyFn
	var toApply []LogEntry
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		toApply = append(toApply, n.log[n.lastApplied-1])
	}
	n.mu.Unlock()

	if fn == nil {
		return
	}
	for _, entry := range toApply {
		fn(entry.Command)
	}
}

// ============================================================
// Propose — client entry point. Only succeeds on the leader.
// Blocks until the entry is committed and applied, or times out.
// ============================================================

func (n *RaftNode) Propose(command string) (int, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: command})
	index := len(n.log)
	n.mu.Unlock()

	go n.broadcastAppendEntries()

	deadline := time.Now().Add(proposeTimeout)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		applied := n.lastApplied >= index
		stillLeader := n.state == Leader
		n.mu.Unlock()
		if applied {
			return index, nil
		}
		if !stillLeader {
			return 0, ErrNotLeader
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, ErrCommitTimeout
}

func (n *RaftNode) LeaderId() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderId
}

// RequestVote RPC
type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntries RPC
type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
