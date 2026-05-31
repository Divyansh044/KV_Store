package main
import (
	"errors"
	"fmt"
	"github.com/Divyansh044/KV_Store/store"
)

func main() {
	s := store.NewKVStore("mystore")
	fmt.Println(s.Info())

	// --- SET three keys ---
	keys := map[string]string{
		"name": "Divyansh",
		"city": "Bengaluru",
		"role": "Software Engineer",
	}
	for k, v := range keys {
		if err := s.Set(k, v); err != nil {
			fmt.Printf("SET error: %v\n", err)
		}
	}
	fmt.Println(s.Info()) // should show 3 keys

	// --- GET all three ---
	fmt.Println("\n-- GET --")
	for _, k := range []string{"name", "city", "role"} {
		val, err := s.Get(k)
		if err != nil {
			fmt.Printf("GET %-6s → error: %v\n", k, err)
		} else {
			fmt.Printf("GET %-6s → %s\n", k, val)
		}
	}

	// --- DELETE one key ---
	fmt.Println("\n-- DELETE --")
	if err := s.Delete("city"); err != nil {
		fmt.Println("Delete error:", err)
	} else {
		fmt.Println("Deleted 'city'")
	}

	// --- GET deleted key — should be ErrKeyNotFound ---
	fmt.Println("\n-- GET after DELETE --")
	_, err := s.Get("city")
	if errors.Is(err, store.ErrKeyNotFound) {
		fmt.Println("'city' not found — was deleted ✓")
	}

	// --- DELETE already-deleted key ---
	err = s.Delete("city")
	if errors.Is(err, store.ErrKeyNotFound) {
		fmt.Println("Can't delete 'city' again — already gone ✓")
	}

	// --- SET with empty key — should be ErrEmptyKey ---
	fmt.Println("\n-- INVALID OPERATIONS --")
	err = s.Set("", "somevalue")
	if errors.Is(err, store.ErrEmptyKey) {
		fmt.Println("Empty key rejected ✓")
	}

	// --- GET with empty key ---
	_, err = s.Get("")
	if errors.Is(err, store.ErrEmptyKey) {
		fmt.Println("Empty key GET rejected ✓")
	}

	fmt.Println("\n-- FINAL STATE --")
	fmt.Println(s.Info())
}