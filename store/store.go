package store

import (
	"errors"
	"fmt"
)

// ============================================================
// Sentinel errors — package level, used across all operations
// ============================================================

var ErrKeyNotFound = errors.New("key not found")
var ErrEmptyKey    = errors.New("key cannot be empty")

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
}

// NewKVStore creates a ready-to-use KV store.
// Always use this — never create KVStore{} directly.
func NewKVStore(name string) *KVStore {
	return &KVStore{
		name: name,
		data: make(map[string]string),
	}
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
	k.data[key] = value
	return nil
}

// Get retrieves a value by key.
// Returns ErrKeyNotFound if the key doesn't exist.
func (k *KVStore) Get(key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
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
	if _, exists := k.data[key]; !exists {
		return fmt.Errorf("Delete(%s): %w", key, ErrKeyNotFound)
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
	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	return keys
}
