package methods

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type StoreValue struct {
	Value    []byte
	ExpireAt time.Time // Zero time means no expiration
}

var nullTimeStamp = time.Time{}

func Echo(commands resp.Value, conn net.Conn) {
	if len(commands.Array) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
		return
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		conn.Write([]byte("-ERR echo value must be a string\r\n"))
		return
	}
	value, err := resp.ParseValue(commands.Array[1])
	if err != nil {
		conn.Write([]byte("-ERR Err parsing echo value: " + err.Error() + "\r\n"))
		return
	}
	conn.Write(value)
}

func Set(commands resp.Value, conn net.Conn, mu *sync.Mutex, store map[string]StoreValue, expiryMap map[time.Time]string) {
	if len(commands.Array) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		return
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		conn.Write([]byte("-ERR set key must be a string\r\n"))
		return
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
		conn.Write([]byte("-ERR set value must be a string\r\n"))
		return
	}
	key := commands.Array[1].String

	expiration := time.Duration(0)
	if len(commands.Array) == 5 {
		if commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
			conn.Write([]byte("-ERR set expiration type must be a string\r\n"))
			return
		}

		if commands.Array[4].Type != resp.RESPTypeInteger && commands.Array[4].Type != resp.RESPTypeBulkString && commands.Array[4].Type != resp.RESPTypeSimpleString {
			conn.Write([]byte("-ERR set expiration value must be an integer\r\n"))
			return
		}
		expirationValue := 0
		if commands.Array[4].Type == resp.RESPTypeInteger {
			expirationValue = commands.Array[4].Integer
		} else {
			eval, err := strconv.Atoi(commands.Array[4].String)
			if err != nil {
				conn.Write([]byte("-ERR set expiration value must be an integer\r\n"))
				return
			}
			expirationValue = eval
		}
		if strings.ToUpper(commands.Array[3].String) == "EX" {
			expiration = time.Second * time.Duration(expirationValue)
		} else if strings.ToUpper(commands.Array[3].String) == "PX" {
			expiration = time.Millisecond * time.Duration(expirationValue)
		} else {
			conn.Write([]byte("-ERR set expiration type must be 'EX' or 'PX'\r\n"))
			return
		}
	}

	value, err := resp.ParseValue(commands.Array[2])
	if err != nil {
		conn.Write([]byte("-ERR Err parsing set value: " + err.Error() + "\r\n"))
		return
	}

	// fmt.Printf("SET %q %q %v\n", key, value, expiration)
	mu.Lock()
	if val, exists := store[key]; exists {
		if val.ExpireAt != nullTimeStamp {
			delete(expiryMap, val.ExpireAt)
		}
	}
	if expiration > 0 {
		timeStamp := time.Now().Add(expiration)
		store[key] = StoreValue{Value: value, ExpireAt: timeStamp}
		expiryMap[timeStamp] = key
	} else {
		store[key] = StoreValue{Value: value, ExpireAt: nullTimeStamp}
	}
	mu.Unlock()
	conn.Write(resp.ToSimpleString("OK"))
}

func Get(commands resp.Value, conn net.Conn, mu *sync.Mutex, store map[string]StoreValue, expiryMap map[time.Time]string) {
	if len(commands.Array) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		if len(commands.Array) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
			return
		}
		if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
			conn.Write([]byte("-ERR get key must be a string\r\n"))
			return
		}
		key := commands.Array[1].String

		mu.Lock()
		if val, exists := store[key]; exists {
			// fmt.Printf("GET %q %q %v\n", key, val.Value, val.ExpireAt)
			if val.ExpireAt != nullTimeStamp && val.ExpireAt.Before(time.Now()) {
				delete(expiryMap, val.ExpireAt)
				delete(store, key)
				mu.Unlock()
				conn.Write(resp.ToBulkString(""))
				return
			}
			value := val.Value
			mu.Unlock()
			conn.Write(value)
		} else {
			mu.Unlock()
			conn.Write(resp.ToBulkString(""))
		}
	}
}
