package main

import (
    "bufio"
    "errors"
    "fmt"
    "os"
    "strings"
    "github.com/Divyansh044/KV_Store/store"
    "github.com/Divyansh044/KV_Store/wal"
)

func main() {
    // create WAL first
    w, err := wal.NewWAL("wal.log")
    if err != nil {
        fmt.Println("failed to open WAL:", err)
        os.Exit(1)
    }

    // create store with WAL
    s := store.NewKVStore("mystore", w)

    // replay WAL to restore state from last run
    if err := w.Replay(s); err != nil {
        fmt.Println("failed to replay WAL:", err)
        os.Exit(1)
    }

    fmt.Println("KVStore[mystore] started. Type commands below.")
    fmt.Println("Commands: SET <key> <value> | GET <key> | DEL <key> | LIST | EXIT")
    fmt.Println(s.Info())
    fmt.Println()

    scanner := bufio.NewScanner(os.Stdin)

    for {
        fmt.Print("> ")
        if !scanner.Scan() {
            break
        }
        line := strings.TrimSpace(scanner.Text())
        if line == "" {
            continue
        }
        parts := strings.Fields(line)
        cmd := strings.ToUpper(parts[0])

        switch cmd {
        case "SET":
            if len(parts) < 3 {
                fmt.Println("ERROR: usage: SET <key> <value>")
                continue
            }
            if err := s.Set(parts[1], parts[2]); err != nil {
                fmt.Println("ERROR:", err)
            } else {
                fmt.Println("OK")
            }

        case "GET":
            if len(parts) < 2 {
                fmt.Println("ERROR: usage: GET <key>")
                continue
            }
            val, err := s.Get(parts[1])
            if errors.Is(err, store.ErrKeyNotFound) {
                fmt.Println("ERROR: key not found")
            } else if err != nil {
                fmt.Println("ERROR:", err)
            } else {
                fmt.Println(val)
            }

        case "DEL":
            if len(parts) < 2 {
                fmt.Println("ERROR: usage: DEL <key>")
                continue
            }
            if err := s.Delete(parts[1]); err != nil {
                if errors.Is(err, store.ErrKeyNotFound) {
                    fmt.Println("ERROR: key not found")
                } else {
                    fmt.Println("ERROR:", err)
                }
            } else {
                fmt.Println("OK")
            }

        case "LIST":
            keys := s.Keys()
            if len(keys) == 0 {
                fmt.Println("(empty)")
            } else {
                for _, k := range keys {
                    val, _ := s.Get(k)
                    fmt.Printf("  %s → %s\n", k, val)
                }
            }

        case "EXIT":
            fmt.Println("bye")
            return

        default:
            fmt.Printf("ERROR: unknown command '%s'\n", parts[0])
        }
    }
}