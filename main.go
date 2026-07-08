package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Divyansh044/KV_Store/raft"
	"github.com/Divyansh044/KV_Store/snapshot"
	"github.com/Divyansh044/KV_Store/store"
	"github.com/Divyansh044/KV_Store/tcp"
	"github.com/Divyansh044/KV_Store/wal"
)

func main() {
	id := flag.String("id", "node1", "unique id for this node")
	raftAddr := flag.String("raft-addr", "localhost:7001", "this node's Raft RPC address (host:port)")
	tcpAddr := flag.String("tcp-addr", ":6379", "this node's client-facing TCP address")
	peersFlag := flag.String("peers", "", "other nodes as id=host:port,id2=host:port,...")
	flag.Parse()

	peers, err := raft.ParsePeers(*peersFlag)
	if err != nil {
		fmt.Println("invalid -peers:", err)
		os.Exit(1)
	}

	walPath := fmt.Sprintf("wal-%s.log", *id)
	snapshotPath := fmt.Sprintf("snapshot-%s.db", *id)

	// create WAL
	w, err := wal.NewWAL(walPath)
	if err != nil {
		fmt.Println("failed to open WAL:", err)
		os.Exit(1)
	}

	// create store with WAL, restore local state from snapshot + WAL replay.
	// This recovers this node's state machine on restart; it is independent of
	// (and not a substitute for) the replicated Raft log below.
	s := store.NewKVStore(*id, w)
	if err := snapshot.Load(s, snapshotPath); err != nil {
		fmt.Println("failed to load snapshot:", err)
		os.Exit(1)
	}
	if err := w.Replay(s); err != nil {
		fmt.Println("failed to replay WAL:", err)
		os.Exit(1)
	}

	// create the Raft node and point committed-entry application at the store.
	// store.Set/store.Delete already do "write WAL + mutate map under lock" —
	// exactly what applying a committed Raft entry needs, so no store changes
	// were needed to hook this up.
	node := raft.NewRaftNode(*id, peers)
	node.SetApplyFunc(func(command string) {
		parts := strings.Fields(command)
		if len(parts) == 0 {
			return
		}
		switch parts[0] {
		case "SET":
			if len(parts) == 3 {
				if err := s.Set(parts[1], parts[2]); err != nil {
					fmt.Println("apply SET failed:", err)
				}
			}
		case "DEL":
			if len(parts) == 2 {
				if err := s.Delete(parts[1]); err != nil {
					fmt.Println("apply DEL failed:", err)
				}
			}
		}
	})

	go func() {
		if err := node.StartRaftServer(*raftAddr); err != nil {
			fmt.Println("raft server error:", err)
			os.Exit(1)
		}
	}()
	node.Run()

	// start TCP server (blocking) — writes are proposed to Raft, reads served locally
	if err := tcp.StartServer(*tcpAddr, s, node, snapshotPath, walPath); err != nil {
		fmt.Println("server error:", err)
		os.Exit(1)
	}
}

