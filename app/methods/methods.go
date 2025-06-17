package methods

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var nullTimeStamp = time.Time{}

func Echo(commands resp.Value) []byte {
	if len(commands.Array) < 2 {
		return resp.ToError("wrong number of arguments for 'echo' command")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return resp.ToError("echo value must be a string")
	}
	value, err := resp.ParseValue(commands.Array[1])
	if err != nil {
		return resp.ToError("Err parsing echo value: " + err.Error())
	}
	return value
}

func Set(commands resp.Value, mu *sync.Mutex, db *resp.Database) (respBytes []byte) {
	if len(commands.Array) < 3 {
		return resp.ToError("wrong number of arguments for 'set' command")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return resp.ToError("set key must be a string")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
		return resp.ToError("set value must be a string")
	}

	expiration := time.Duration(0)
	if len(commands.Array) == 5 {
		if commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
			return resp.ToError("set expiration type must be a string")
		}

		if commands.Array[4].Type != resp.RESPTypeInteger && commands.Array[4].Type != resp.RESPTypeBulkString && commands.Array[4].Type != resp.RESPTypeSimpleString {
			return resp.ToError("set expiration value must be an integer")
		}
		expirationValue := 0
		if commands.Array[4].Type == resp.RESPTypeInteger {
			expirationValue = commands.Array[4].Integer
		} else {
			eval, err := strconv.Atoi(commands.Array[4].String)
			if err != nil {
				return resp.ToError("set expiration value must be an integer")
			}
			expirationValue = eval
		}
		if strings.ToUpper(commands.Array[3].String) == "EX" {
			expiration = time.Second * time.Duration(expirationValue)
		} else if strings.ToUpper(commands.Array[3].String) == "PX" {
			expiration = time.Millisecond * time.Duration(expirationValue)
		} else {
			return resp.ToError("set expiration type must be 'EX' or 'PX'")
		}
	}

	mu.Lock()
	defer mu.Unlock()

	key := commands.Array[1].String
	if key == "" {
		return resp.ToError("empty key")
	}

	// Check if key exists and clean up old expiry if needed
	if oldVal, exists := (*db).Store[key]; exists {
		if !oldVal.ExpireAt.IsZero() {
			delete((*db).ExpiryMap, oldVal.ExpireAt)
		}
	}

	// Set new value and expiry
	if expiration != time.Duration(0) {
		timeStamp := time.Now().Add(expiration)
		(*db).Store[key] = resp.StoreValue{
			Value:    commands.Array[2],
			ExpireAt: timeStamp,
		}
		(*db).ExpiryMap[timeStamp] = key
	} else {
		// fmt.Println("SET", key, commands.Array[2].String, expiration)
		(*db).Store[key] = resp.StoreValue{
			Value:    commands.Array[2],
			ExpireAt: nullTimeStamp,
		}
	}
	return resp.ToSimpleString("OK")
}

func Get(commands resp.Value, mu *sync.Mutex, db *resp.Database) []byte {
	if len(commands.Array) < 2 {
		return resp.ToError("wrong number of arguments for 'get' command")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return resp.ToError("get key must be a string")
	}
	key := commands.Array[1].String

	mu.Lock()
	defer mu.Unlock()

	if val, exists := (*db).Store[key]; exists {
		// fmt.Printf("GET %q %q %v\n", key, val.Value, val.ExpireAt)
		if !val.ExpireAt.IsZero() && val.ExpireAt.Before(time.Now()) {
			delete((*db).ExpiryMap, val.ExpireAt)
			delete((*db).Store, key)
			return resp.ToBulkString("")
		}
		value := val.Value

		BytesValue, err := resp.ParseValue(value)
		if err != nil {
			return resp.ToError("Err parsing value: " + err.Error())
		}

		return BytesValue
	} else {
		return resp.ToBulkString("")
	}
}

func Keys(commands resp.Value, mu *sync.Mutex, db *resp.Database) []byte {
	if len(commands.Array) < 2 {
		return resp.ToError("wrong number of arguments for 'keys' command")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return resp.ToError("keys pattern must be a string")
	}
	var pattern strings.Builder

	for _, c := range commands.Array[1].String {
		switch c {
		case '*':
			pattern.WriteString(".*")
		case '?':
			pattern.WriteString(".")
		default:
			pattern.WriteRune(c)
		}
	}

	regexPattern := pattern.String()

	mu.Lock()
	defer mu.Unlock()

	keys := make([]any, 0)
	for key := range (*db).Store {
		match, err := regexp.MatchString(regexPattern, key)
		if err != nil {
			return resp.ToError("Err matching pattern: " + err.Error())
		}
		if match {
			keys = append(keys, key)
		}
	}
	return resp.ToArray(keys)
}

func HandleConfig(commands resp.Value, mu *sync.Mutex, config map[string]string) []byte {
	if len(commands.Array) < 2 {
		return resp.ToError("wrong number of arguments for 'config' command")
	}
	if commands.Array[1].Type != resp.RESPTypeBulkString && commands.Array[1].Type != resp.RESPTypeSimpleString {
		return resp.ToError("config key must be a string")
	}
	key := commands.Array[1].String
	switch strings.ToUpper(key) {
	case "GET":
		return ConfigGet(commands, mu, config)
	case "SET":
		return ConfigSet(commands, mu, config)
	default:
		return resp.ToError("unknown config command")
	}
}

func ConfigGet(commands resp.Value, mu *sync.Mutex, config map[string]string) []byte {
	if len(commands.Array) < 2 {
		return resp.ToError("wrong number of arguments for 'config' command")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString {
		return resp.ToError("config key must be a string")
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
		return resp.ToError("wrong number of arguments for 'config' command")
	}
	if commands.Array[2].Type != resp.RESPTypeBulkString && commands.Array[2].Type != resp.RESPTypeSimpleString && commands.Array[3].Type != resp.RESPTypeBulkString && commands.Array[3].Type != resp.RESPTypeSimpleString {
		return resp.ToError("config key and value must be a string")
	}
	key := commands.Array[2].String
	value := commands.Array[3].String
	mu.Lock()
	config[key] = value
	mu.Unlock()
	return resp.ToBulkString("OK")
}
