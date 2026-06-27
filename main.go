package main

import (
	"fmt"
	"github.com/Divyansh044/KV_Store/snapshot"
	"github.com/Divyansh044/KV_Store/store"
	"github.com/Divyansh044/KV_Store/tcp"
	"github.com/Divyansh044/KV_Store/wal"
	"os"
)

func main() {
	// create WAL
	w, err := wal.NewWAL("wal.log")
	if err != nil {
		fmt.Println("failed to open WAL:", err)
		os.Exit(1)
	}

	// create store with WAL
	s := store.NewKVStore("mystore", w)
	if err := snapshot.Load(s, "snapshot.db"); err != nil {
		fmt.Println("failed to load snapshot:", err)
		os.Exit(1)
	}

	// replay WAL — restore state from last run
	if err := w.Replay(s); err != nil {
		fmt.Println("failed to replay WAL:", err)
		os.Exit(1)
	}

	// start TCP server
	if err := tcp.StartServer(":6379", s); err != nil {
		fmt.Println("server error:", err)
		os.Exit(1)
	}
}
