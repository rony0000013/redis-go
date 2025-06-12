package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	mu    sync.Mutex
	store = make(map[string][]byte)
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

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

		// fmt.Printf("Command: %q ", command.String)
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
			key, err := resp.ParseValue(commands.Array[1])
			if err != nil {
				conn.Write([]byte("-ERR Err parsing set key: " + err.Error() + "\r\n"))
				continue
			}
			value, err := resp.ParseValue(commands.Array[2])
			if err != nil {
				conn.Write([]byte("-ERR Err parsing set value: " + err.Error() + "\r\n"))
				continue
			}

			mu.Lock()
			store[string(key)] = value
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
			key, err := resp.ParseValue(commands.Array[1])
			if err != nil {
				conn.Write([]byte("-ERR Err parsing get key: " + err.Error() + "\r\n"))
				continue
			}

			mu.Lock()
			value := store[string(key)]
			mu.Unlock()
			fmt.Printf("GET %q %q\n", string(key), string(value))
			conn.Write(value)
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
