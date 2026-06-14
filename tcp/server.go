package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Divyansh044/KV_Store/store"
)

func StartServer(addr string, s *store.KVStore) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer listener.Close()
	fmt.Printf("KVStore TCP server started on %s\n", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go handleClient(conn, s)
	}
}
func handleClient(conn net.Conn, s *store.KVStore) {
	defer conn.Close()
	fmt.Fprintf(conn, "Welcome to KVStore TCP server!\n")
	fmt.Fprintf(conn, "Commands: SET <key> <value> | GET <key> | DEL <key> | LIST | EXIT\n")
	scanner := bufio.NewScanner(conn)
	for {
		fmt.Fprintf(conn, "> ")
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
				fmt.Fprintf(conn, "ERROR: usage: SET <key> <value>\n")
				continue
			}
			if err := s.Set(parts[1], parts[2]); err != nil {
				fmt.Fprintf(conn, "ERROR: %v\n", err)
			} else {
				fmt.Fprintf(conn, "OK\n")
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "ERROR: usage: GET <key>\n")
				continue
			}
			val, err := s.Get(parts[1])
			if errors.Is(err, store.ErrKeyNotFound) {
				fmt.Fprintf(conn, "ERROR: key not found\n")
			} else if err != nil {
				fmt.Fprintf(conn, "ERROR: %v\n", err)
			} else {
				fmt.Fprintf(conn, "%s\n", val)
			}

		case "DEL":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "ERROR: usage: DEL <key>\n")
				continue
			}
			if err := s.Delete(parts[1]); err != nil {
				if errors.Is(err, store.ErrKeyNotFound) {
					fmt.Fprintf(conn, "ERROR: key not found\n")
				} else {
					fmt.Fprintf(conn, "ERROR: %v\n", err)
				}
			} else {
				fmt.Fprintf(conn, "OK\n")
			}

		case "LIST":
			keys := s.Keys()
			if len(keys) == 0 {
				fmt.Fprintf(conn, "(empty)\n")
			} else {
				for _, k := range keys {
					val, _ := s.Get(k)
					fmt.Fprintf(conn, "  %s -> %s\n", k, val)
				}
			}

		case "EXIT":
			fmt.Fprintf(conn, "bye\n")
			return

		default:
			fmt.Fprintf(conn, "ERROR: unknown command '%s'\n", parts[0])
		}
	}
}
