package wal

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
	file *os.File
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(entry string) error {
	_, err := fmt.Fprintf(w.file, "%s\n", entry)
	return err
}

type Applier interface {
	ApplySet(key, value string) error
	ApplyDelete(key string) error
}

func (w *WAL) Replay(a Applier) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("WAL replay seek failed: %w", err)
	}
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if parts[0] == "SET" && len(parts) == 3 {
			a.ApplySet(parts[1], parts[2])
		}
		if parts[0] == "DEL" && len(parts) == 2 {
			a.ApplyDelete(parts[1])
		}

	}
	return nil
}
