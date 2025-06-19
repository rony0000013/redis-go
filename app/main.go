package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/methods"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	mu         sync.Mutex
	Databases        = make(map[uint8]resp.Database)
	DatabaseID uint8 = 0
	Config           = make(map[string]string)
)

func main() {
	dir_flag := flag.String("dir", "", "Directory to store data")
	dbfilename_flag := flag.String("dbfilename", "", "File to store data")
	flag.Parse()

	if *dir_flag != "" {
		// err := os.MkdirAll(*dir_flag, 0755)
		// if err != nil {
		// 	fmt.Println("Failed to create directory: ", err.Error())
		// 	os.Exit(1)
		// }
		Config["dir"] = *dir_flag
	}
	if *dbfilename_flag != "" {
		// filePath := filepath.Join(*dir_flag, *dbfilename_flag)
		// _, err := os.Create(filePath)
		// if err != nil {
		// 	fmt.Println("Failed to create file: ", err.Error())
		// 	os.Exit(1)
		// }
		Config["dbfilename"] = *dbfilename_flag
	}

	metadata, databases, err := rdb.Open(Config["dir"], Config["dbfilename"])
	if err != nil {
		fmt.Println("Failed to open database: ", err.Error())
		Databases[DatabaseID] = resp.NewDatabase(DatabaseID)
	} else {
		Config = metadata
		Databases = databases
	}
	Config["redis-version"] = "6.0.16"

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM, os.Kill)

	go startExpiryChecker(stopCh)

	fmt.Println("Server started on port 6379")

	// Start a goroutine to handle server shutdown
	go func() {
		<-stopCh
		fmt.Println("\nShutting down server...")
		os.Exit(0)
	}()

	// Main server loop
	for {
		conn, err := l.Accept()
		if err != nil {
			// If we received a shutdown signal, break the loop
			select {
			case <-stopCh:
				return
			default:
				fmt.Println("Error accepting connection: ", err.Error())
				continue
			}
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
			conn.Write(resp.ToError("parsing RESP: " + err.Error()))
			break
		}

		if len(rest) > 0 {
			conn.Write(resp.ToError("rest data: " + string(rest)))
			continue
		}
		if commands.Type != resp.RESPTypeArray {
			conn.Write(resp.ToError("wrong command structure"))
			continue
		}

		if len(commands.Array) == 0 {
			conn.Write(resp.ToError("no elements in command"))
			continue
		}

		command := commands.Array[0]

		if command.Type != resp.RESPTypeSimpleString && command.Type != resp.RESPTypeBulkString {
			conn.Write(resp.ToError("wrong command type"))
			continue
		}

		db := Databases[DatabaseID]

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
			conn.Write(methods.Echo(commands))
		case "SET":
			conn.Write(methods.Set(commands, &mu, &db))
		case "GET":
			conn.Write(methods.Get(commands, &mu, &db))
		case "KEYS":
			conn.Write(methods.Keys(commands, &mu, &db))
		case "CONFIG":
			conn.Write(methods.HandleConfig(commands, &mu, Config))
		case "SAVE":
			{
				err = rdb.Save(Config["dir"], Config["dbfilename"], Config, Databases)
				if err != nil {
					conn.Write(resp.ToError("Failed to save database: " + err.Error()))
				}
				conn.Write(resp.ToSimpleString("OK"))
			}
		default:
			conn.Write(resp.ToError("unknown command"))
		}
	}
}

func startExpiryChecker(stopCh <-chan os.Signal) {
	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for id, _ := range Databases {
		select {
		case now := <-ticker.C:
			mu.Lock()
			for timestamp, key := range Databases[id].ExpiryMap {
				if now.After(timestamp) {
					if val, exists := Databases[id].Store[key]; exists && val.ExpireAt == timestamp {
						delete(Databases[id].Store, key)
					}

					delete(Databases[id].ExpiryMap, timestamp)
				}
			}
			mu.Unlock()
		case <-stopCh:
			return // Exit the goroutine when stopCh is closed
		}
	}
}
