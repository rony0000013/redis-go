package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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
		len, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from connection:", err.Error())
			break
		}
		commands, err := parseRESP(buf[:len])
		if err != nil {
			fmt.Println("Error parsing RESP:", err.Error())
			break
		}
		// fmt.Println("Received command: ", commands)

		if strings.ToUpper(commands[0]) == "PING" || strings.ToUpper(commands[0]) == "INFO" {
			conn.Write([]byte("+PONG\r\n"))
		} else if strings.ToUpper(commands[0]) == "ECHO" {
			conn.Write([]byte("+" + commands[1] + "\r\n"))
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func parseRESP(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	if data[0] != '*' {
		return nil, fmt.Errorf("expected array type * as first byte")
	}

	endOfFirstLine := bytes.IndexByte(data, '\n')
	if endOfFirstLine == -1 {
		return nil, fmt.Errorf("invalid RESP format: no newline")
	}

	numElements, err := strconv.Atoi(string(data[1 : endOfFirstLine-1])) // -1 to remove \r
	if err != nil {
		return nil, fmt.Errorf("invalid number of elements: %v", err)
	}

	var commands []string
	pos := endOfFirstLine + 1

	for range numElements {
		if pos >= len(data) {
			return nil, fmt.Errorf("incomplete RESP data")
		}

		if data[pos] != '$' {
			return nil, fmt.Errorf("expected $ at position %d", pos)
		}

		endOfLen := bytes.IndexByte(data[pos:], '\n')
		if endOfLen == -1 {
			return nil, fmt.Errorf("invalid bulk string format")
		}
		endOfLen += pos

		strLen, err := strconv.Atoi(string(data[pos+1 : endOfLen-1])) // +1 to skip $, -1 to remove \r
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %v", err)
		}

		pos = endOfLen + 1

		if pos+strLen > len(data) {
			return nil, fmt.Errorf("incomplete string data")
		}

		str := string(data[pos : pos+strLen])
		commands = append(commands, str)

		pos += strLen + 2
	}

	return commands, nil
}
