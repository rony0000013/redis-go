package methods

import (
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

func Echo(commands resp.Value) []byte {
	if len(commands.Array) < 2 {
		return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR echo value must be a string\r\n")
	}
	value, err := resp.ParseValue(commands.Array[1])
	if err != nil {
		return []byte("-ERR Err parsing echo value: " + err.Error() + "\r\n")
	}
	return value
}

func Set(commands resp.Value, mu *sync.Mutex, store map[string]StoreValue, expiryMap map[time.Time]string) []byte {
	if len(commands.Array) < 3 {
		return []byte("-ERR wrong number of arguments for 'set' command\r\n")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR set key must be a string\r\n")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR set value must be a string\r\n")
	}
	key := commands.Array[1].String

	expiration := time.Duration(0)
	if len(commands.Array) == 5 {
		if commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
			return []byte("-ERR set expiration type must be a string\r\n")
		}

		if commands.Array[4].Type != resp.RESPTypeInteger && commands.Array[4].Type != resp.RESPTypeBulkString && commands.Array[4].Type != resp.RESPTypeSimpleString {
			return []byte("-ERR set expiration value must be an integer\r\n")
		}
		expirationValue := 0
		if commands.Array[4].Type == resp.RESPTypeInteger {
			expirationValue = commands.Array[4].Integer
		} else {
			eval, err := strconv.Atoi(commands.Array[4].String)
			if err != nil {
				return []byte("-ERR set expiration value must be an integer\r\n")
			}
			expirationValue = eval
		}
		if strings.ToUpper(commands.Array[3].String) == "EX" {
			expiration = time.Second * time.Duration(expirationValue)
		} else if strings.ToUpper(commands.Array[3].String) == "PX" {
			expiration = time.Millisecond * time.Duration(expirationValue)
		} else {
			return []byte("-ERR set expiration type must be 'EX' or 'PX'\r\n")
		}
	}

	value, err := resp.ParseValue(commands.Array[2])
	if err != nil {
		return []byte("-ERR Err parsing set value: " + err.Error() + "\r\n")
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
	return resp.ToSimpleString("OK")
}

func Get(commands resp.Value, mu *sync.Mutex, store map[string]StoreValue, expiryMap map[time.Time]string) []byte {
	if len(commands.Array) < 2 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR get key must be a string\r\n")
	}
	key := commands.Array[1].String

	mu.Lock()
	if val, exists := store[key]; exists {
		// fmt.Printf("GET %q %q %v\n", key, val.Value, val.ExpireAt)
		if val.ExpireAt != nullTimeStamp && val.ExpireAt.Before(time.Now()) {
			delete(expiryMap, val.ExpireAt)
			delete(store, key)
			mu.Unlock()
			return resp.ToBulkString("")
		}
		value := val.Value
		mu.Unlock()
		return value
	} else {
		mu.Unlock()
		return resp.ToBulkString("")
	}
}

func HandleConfig(commands resp.Value, mu *sync.Mutex, config map[string]string) []byte {
	if len(commands.Array) < 2 {
		return []byte("-ERR wrong number of arguments for 'config' command\r\n")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR config key must be a string\r\n")
	}
	key := commands.Array[1].String
	switch strings.ToUpper(key) {
	case "GET":
		return ConfigGet(commands, mu, config)
	case "SET":
		return ConfigSet(commands, mu, config)
	default:
		return []byte("-ERR unknown config command\r\n")
	}
}

func ConfigGet(commands resp.Value, mu *sync.Mutex, config map[string]string) []byte {
	if len(commands.Array) < 2 {
		return []byte("-ERR wrong number of arguments for 'config' command\r\n")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR config key must be a string\r\n")
	}
	key := commands.Array[2].String
	mu.Lock()
	if val, exists := config[key]; exists {
		mu.Unlock()
		return resp.ToArray([]any{key, val})
	} else {
		mu.Unlock()
		return resp.ToBulkString("")
	}
}

func ConfigSet(commands resp.Value, mu *sync.Mutex, config map[string]string) []byte {
	if len(commands.Array) < 3 {
		return []byte("-ERR wrong number of arguments for 'config' command\r\n")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString && commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
		return []byte("-ERR config key and value must be a string\r\n")
	}
	key := commands.Array[2].String
	value := commands.Array[3].String
	mu.Lock()
	config[key] = value
	mu.Unlock()
	return resp.ToBulkString("OK")
}
