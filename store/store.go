package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Divyansh044/KV_Store/wal"
)

// ============================================================
// Sentinel errors — package level, used across all operations
// ============================================================

var ErrKeyNotFound = errors.New("key not found")
var ErrEmptyKey = errors.New("key cannot be empty")

// ============================================================
// Validation
// ============================================================

func validateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}

// ============================================================
// KVStore — the core data structure
// ============================================================

type KVStore struct {
	name string
	data map[string]string
	wal  *wal.WAL
	mu   sync.RWMutex
}

// NewKVStore creates a ready-to-use KV store.
// Always use this — never create KVStore{} directly.
func NewKVStore(name string, w *wal.WAL) *KVStore {
	return &KVStore{
		name: name,
		data: make(map[string]string),
		wal:  w,
	}
}

// these are for replay only, no WAL write
func (k *KVStore) ApplySet(key, value string) error {
	k.data[key] = value
	return nil
}

func (k *KVStore) ApplyDelete(key string) error {
	delete(k.data, key)
	return nil
}
func (k *KVStore) Info() string {
	return fmt.Sprintf("KVStore[%s] — %d keys", k.name, len(k.data))
}

func (k *KVStore) Rename(newName string) {
	k.name = newName
}

// Set stores a key-value pair.
// Returns ErrEmptyKey if key is blank.
func (k *KVStore) Set(key, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.wal != nil {
		entry := fmt.Sprintf("SET %s %s", key, value)
		if err := k.wal.Append(entry); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}
	}
	k.data[key] = value
	return nil
}

// Get retrieves a value by key.
// Returns ErrKeyNotFound if the key doesn't exist.
func (k *KVStore) Get(key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	value, exists := k.data[key]
	if !exists {
		return "", fmt.Errorf("Get(%s): %w", key, ErrKeyNotFound)
	}
	return value, nil
}

// Delete removes a key from the store.
// Returns ErrKeyNotFound if the key doesn't exist.
func (k *KVStore) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, exists := k.data[key]; !exists {
		return fmt.Errorf("Delete(%s): %w", key, ErrKeyNotFound)
	}
	if k.wal != nil {
		entry := fmt.Sprintf("DEL %s", key)
		if err := k.wal.Append(entry); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}
	}
	delete(k.data, key)
	return nil
}

// ============================================================
// Command — represents a parsed client instruction
// ============================================================

type Command struct {
	operation string // SET / GET / DEL
	key       string
	value     string
}

func (c Command) String() string {
	return fmt.Sprintf("%s key=%s value=%s", c.operation, c.key, c.value)
}

// Keys returns all keys in the store
func (k *KVStore) Keys() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	return keys
}
