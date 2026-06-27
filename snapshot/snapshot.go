package snapshot

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Divyansh044/KV_Store/store"
)

func Save(s *store.KVStore, snapshotPath string, walPath string) error {
	// 1. get snapshot data
	data := s.Snapshot()

	// 2. open snapshot file — O_CREATE|O_WRONLY|O_TRUNC
	//    O_TRUNC means overwrite from beginning — not append
	file, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	// 3. write each key-value pair
	//    fmt.Fprintf(file, "%s %s\n", key, val)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()
	for key, val := range data {
		if _, err := fmt.Fprintf(file, "%s %s\n", key, val); err != nil {
			return fmt.Errorf("failed to write to snapshot file: %w", err)
		}
	}
	// 4. truncate WAL
	//    os.Truncate(walPath, 0)
	if err := os.Truncate(walPath, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}
	// 5. return nil
	return nil
}
func Load(s *store.KVStore, snapshotPath string) error {
	// 1. check if file exists — if not, return nil
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		return nil
	}
	// 2. open file for reading — os.Open(snapshotPath)
	file, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()
	// 3. bufio.Scanner on file
	scanner := bufio.NewScanner(file)
	// 4. for scanner.Scan():
	//        line := scanner.Text()
	//        parts := strings.Fields(line)
	//        if len(parts) != 2 → continue
	//        s.ApplySet(parts[0], parts[1])
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		s.ApplySet(parts[0], parts[1])
	}
	// 6. return nil
	return nil
}
