package raft
import(
	"sync"
)
type NodeState int

const(
	Follower NodeState = iota
	Candidate
	Leader
)
type LogEntry struct {
	Term int
	Command string
}

type RaftNode struct{
	id string
	peers []string
	currentTerm int
	votedFor string
	log []LogEntry
	state NodeState
	mu sync.Mutex
}
func NewRaftNode(id string, peers []string) *RaftNode {
    return &RaftNode{
        id:          id,
        peers:       peers,
        currentTerm: 0,
        votedFor:    "",
        state:       Follower,
        log:         []LogEntry{},
    }
}