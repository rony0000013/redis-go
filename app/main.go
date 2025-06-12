package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/methods"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	mu        sync.Mutex
	store     = make(map[string]methods.StoreValue)
	config    = make(map[string]string)
	expiryMap = make(map[time.Time]string)
)

func main() {
	dir_flag := flag.String("dir", "", "Directory to store data")
	dbfilename_flag := flag.String("dbfilename", "", "File to store data")
	flag.Parse()

	if *dir_flag != "" {
		err := os.MkdirAll(*dir_flag, 0755)
		if err != nil {
			fmt.Println("Failed to create directory: ", err.Error())
			os.Exit(1)
		}
		config["dir"] = *dir_flag
	}
	if *dbfilename_flag != "" {
		filePath := filepath.Join(*dir_flag, *dbfilename_flag)
		err := os.MkdirAll(filepath.Dir(filePath), 0755)
		if err != nil {
			fmt.Println("Failed to create directory: ", err.Error())
			os.Exit(1)
		}
		config["dbfilename"] = *dbfilename_flag
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	stopCh := make(chan struct{})
	go startExpiryChecker(stopCh)

	fmt.Println("Server started on port 6379")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Client connected: ", conn.RemoteAddr().String())

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from connection:", err.Error())
			}
			break
		}
		// fmt.Printf("Command: %q\n", string(buf[:n]))
		commands, rest, err := resp.Parse(buf[:n])
		if err != nil {
			fmt.Println("ERR: parsing RESP:", err.Error())
			break
		}

		if len(rest) > 0 {
			fmt.Printf("ERR: rest data: %q\n", string(rest))
			continue
		}
		if commands.Type != resp.RESPTypeArray {
			conn.Write([]byte("-ERR wrong command structure\r\n"))
			continue
		}

		if len(commands.Array) == 0 {
			conn.Write([]byte("-ERR no elements in command\r\n"))
			continue
		}

		command := commands.Array[0]

		if command.Type != resp.RESPTypeSimpleString && command.Type != resp.RESPTypeBulkString {
			conn.Write([]byte("-ERR wrong command type\r\n"))
			continue
		}

		// fmt.Printf("Command: ")
		// for _, value := range commands.Array {
		// 	fmt.Printf("%q ", value.String)
		// }
		// fmt.Println()
		switch strings.ToUpper(command.String) {
		case "PING":
			conn.Write(resp.ToSimpleString("PONG"))
		case "INFO":
			conn.Write(resp.ToSimpleString("PONG"))
		case "ECHO":
			methods.Echo(commands, conn)
		case "SET":
			methods.Set(commands, conn, &mu, store, expiryMap)
		case "GET":
			methods.Get(commands, conn, &mu, store, expiryMap)
		case "CONFIG":
			methods.HandleConfig(commands, conn, &mu, config)
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func startExpiryChecker(stopCh <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			mu.Lock()
			for timestamp, key := range expiryMap {
				if now.After(timestamp) {
					if val, exists := store[key]; exists && val.ExpireAt == timestamp {
						delete(store, key)
					}

					delete(expiryMap, timestamp)
				}
			}
			mu.Unlock()
		case <-stopCh:
			return // Exit the goroutine when stopCh is closed
		}
	}
}
