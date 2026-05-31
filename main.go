package main

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

// ============================================================
// Main — demo of the working KV store
// ============================================================

func main() {
	store := NewKVStore("mystore")
	fmt.Println(store.Info())

	// --- SET three keys ---
	keys := map[string]string{
		"name": "Divyansh",
		"city": "Bengaluru",
		"role": "Software Engineer",
	}
	for k, v := range keys {
		if err := store.Set(k, v); err != nil {
			fmt.Printf("SET error: %v\n", err)
		}
	}
	fmt.Println(store.Info()) // should show 3 keys

	// --- GET all three ---
	fmt.Println("\n-- GET --")
	for _, k := range []string{"name", "city", "role"} {
		val, err := store.Get(k)
		if err != nil {
			fmt.Printf("GET %-6s → error: %v\n", k, err)
		} else {
			fmt.Printf("GET %-6s → %s\n", k, val)
		}
	}

	// --- DELETE one key ---
	fmt.Println("\n-- DELETE --")
	if err := store.Delete("city"); err != nil {
		fmt.Println("Delete error:", err)
	} else {
		fmt.Println("Deleted 'city'")
	}

	// --- GET deleted key — should be ErrKeyNotFound ---
	fmt.Println("\n-- GET after DELETE --")
	_, err := store.Get("city")
	if errors.Is(err, ErrKeyNotFound) {
		fmt.Println("'city' not found — was deleted ✓")
	}

	// --- DELETE already-deleted key ---
	err = store.Delete("city")
	if errors.Is(err, ErrKeyNotFound) {
		fmt.Println("Can't delete 'city' again — already gone ✓")
	}

	// --- SET with empty key — should be ErrEmptyKey ---
	fmt.Println("\n-- INVALID OPERATIONS --")
	err = store.Set("", "somevalue")
	if errors.Is(err, ErrEmptyKey) {
		fmt.Println("Empty key rejected ✓")
	}

	// --- GET with empty key ---
	_, err = store.Get("")
	if errors.Is(err, ErrEmptyKey) {
		fmt.Println("Empty key GET rejected ✓")
	}

	fmt.Println("\n-- FINAL STATE --")
	fmt.Println(store.Info())
}