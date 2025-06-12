package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type storeValue struct {
	value    []byte
	expireAt time.Time // Zero time means no expiration
}

var (
	mu        sync.Mutex
	store     = make(map[string]storeValue)
	expiryMap = make(map[time.Time]string)
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	// stopCh := make(chan struct{})
	// go startExpiryChecker(stopCh)

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

	nullTimeStamp := time.Time{}

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
		if strings.ToUpper(command.String) == "PING" || strings.ToUpper(command.String) == "INFO" {
			conn.Write(resp.ToSimpleString("PONG"))
		} else if strings.ToUpper(command.String) == "ECHO" {
			if len(commands.Array) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
				conn.Write([]byte("-ERR echo value must be a string\r\n"))
				continue
			}
			value, err := resp.ParseValue(commands.Array[1])
			if err != nil {
				conn.Write([]byte("-ERR Err parsing echo value: " + err.Error() + "\r\n"))
				continue
			}
			conn.Write(value)
		} else if strings.ToUpper(command.String) == "SET" {
			if len(commands.Array) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
				conn.Write([]byte("-ERR set key must be a string\r\n"))
				continue
			}
			if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
				conn.Write([]byte("-ERR set value must be a string\r\n"))
				continue
			}
			key := commands.Array[1].String

			expiration := time.Duration(0)
			if len(commands.Array) == 5 {
				if commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
					conn.Write([]byte("-ERR set expiration type must be a string\r\n"))
					continue
				}

				if commands.Array[4].Type != resp.RESPTypeInteger && commands.Array[4].Type != resp.RESPTypeBulkString && commands.Array[4].Type != resp.RESPTypeSimpleString {
					conn.Write([]byte("-ERR set expiration value must be an integer\r\n"))
					continue
				}
				expirationValue := 0
				if commands.Array[4].Type == resp.RESPTypeInteger {
					expirationValue = commands.Array[4].Integer
				} else {
					eval, err := strconv.Atoi(commands.Array[4].String)
					if err != nil {
						conn.Write([]byte("-ERR set expiration value must be an integer\r\n"))
						continue
					}
					expirationValue = eval
				}
				if strings.ToUpper(commands.Array[3].String) == "EX" {
					expiration = time.Second * time.Duration(expirationValue)
				} else if strings.ToUpper(commands.Array[3].String) == "PX" {
					expiration = time.Millisecond * time.Duration(expirationValue)
				} else {
					conn.Write([]byte("-ERR set expiration type must be 'EX' or 'PX'\r\n"))
					continue
				}
			}

			value, err := resp.ParseValue(commands.Array[2])
			if err != nil {
				conn.Write([]byte("-ERR Err parsing set value: " + err.Error() + "\r\n"))
				continue
			}

			// fmt.Printf("SET %q %q %v\n", key, value, expiration)
			mu.Lock()
			if val, exists := store[key]; exists {
				if val.expireAt != nullTimeStamp {
					delete(expiryMap, val.expireAt)
				}
			}
			if expiration > 0 {
				timeStamp := time.Now().Add(expiration)
				store[key] = storeValue{value: value, expireAt: timeStamp}
				expiryMap[timeStamp] = key
			} else {
				store[key] = storeValue{value: value, expireAt: nullTimeStamp}
			}
			mu.Unlock()
			conn.Write(resp.ToSimpleString("OK"))
		} else if strings.ToUpper(command.String) == "GET" {
			if len(commands.Array) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
				conn.Write([]byte("-ERR get key must be a string\r\n"))
				continue
			}
			key := commands.Array[1].String

			mu.Lock()
			if val, exists := store[key]; exists {
				// fmt.Printf("GET %q %q %v\n", key, val.value, val.expireAt)
				if val.expireAt != nullTimeStamp && val.expireAt.Before(time.Now()) {
					delete(expiryMap, val.expireAt)
					delete(store, key)
					mu.Unlock()
					conn.Write(resp.ToBulkString(""))
					continue
				}
				value := val.value
				mu.Unlock()
				conn.Write(value)
			} else {
				mu.Unlock()
				conn.Write(resp.ToBulkString(""))
			}
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

// func startExpiryChecker(stopCh <-chan struct{}) {
// 	ticker := time.NewTicker(1 * time.Second) // Check every second
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case now := <-ticker.C:
// 			mu.Lock()
// 			for timestamp, key := range expiryMap {
// 				if now.After(timestamp) {
// 					if val, exists := store[key]; exists && val.expireAt == timestamp {
// 						delete(store, key)
// 					}

// 					delete(expiryMap, timestamp)
// 				}
// 			}
// 			mu.Unlock()
// 		case <-stopCh:
// 			return // Exit the goroutine when stopCh is closed
// 		}
// 	}
// }
