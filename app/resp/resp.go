package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

type RESP byte

const (
	RESPTypeArray        RESP = '*'
	RESPTypeBulkString   RESP = '$'
	RESPTypeSimpleString RESP = '+'
	RESPTypeInteger      RESP = ':'
	RESPTypeNull         RESP = '_'
	RESPTypeError        RESP = '-'
)

type Value struct {
	Type    RESP
	String  string
	Integer int
	Array   []Value
	IsNull  bool
}

func Parse(data []byte) (Value, []byte, error) {
	switch data[0] {
	case '_':
		return Value{Type: RESPTypeNull}, []byte(nil), nil
	case '+':
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid basic string format: no newline")
			}
			return Value{Type: RESPTypeSimpleString, String: string(data[1 : len-1])}, data[len+1:], nil
		}
	case ':':
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid integer format: no newline")
			}
			integer, err := strconv.Atoi(string(data[1 : len-1]))
			if err != nil {
				return Value{}, []byte(nil), fmt.Errorf("invalid integer: %v", err)
			}
			return Value{Type: RESPTypeInteger, Integer: integer}, data[len+1:], nil
		}
	case '$':
		{
			// fmt.Printf("Bulk string data: %q\n", string(data))
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid bulk string format: no newline")
			}
			end, err := strconv.Atoi(string(data[1 : len-1]))
			if err != nil {
				return Value{}, []byte(nil), fmt.Errorf("invalid bulk string size: %v", err)
			}
			value := string(data[len+1 : len+1+end])
			// fmt.Printf("Bulk string value: %q\n", string(value))
			return Value{Type: RESPTypeBulkString, String: value}, data[len+end+3:], nil
		}
	case '*':
		{
			length := bytes.IndexByte(data, '\n')
			if length == -1 || length == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid array format: no newline")
			}
			n, err := strconv.Atoi(string(data[1 : length-1]))
			if err != nil {
				return Value{}, []byte(nil), fmt.Errorf("invalid array size: %v", err)
			}
			buf := data[length+1:]
			var array []Value
			for range n {
				// fmt.Printf("Array element: %q\n", string(buf))
				value, rest, err := Parse(buf)
				if err != nil {
					return Value{}, []byte(nil), fmt.Errorf("invalid array element: %v", err)
				}
				array = append(array, value)
				buf = rest
			}
			if len(buf) > 0 {
				return Value{}, []byte(nil), fmt.Errorf("not enough elements for array")
			}
			return Value{Type: RESPTypeArray, Array: array}, buf, nil
		}
	case '-':
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid error format: no newline")
			}
			return Value{Type: RESPTypeError, String: string(data[1 : len-1])}, data[len+1:], nil
		}
	default:
		return Value{}, []byte(nil), fmt.Errorf("unknown type: %c and data: %s", data[0], string(data))
	}
}

func ParseValue(data Value) ([]byte, error) {
	switch data.Type {
	case RESPTypeArray:
		{
			var buf []byte
			buf = append(buf, '*')
			buf = append(buf, []byte(strconv.Itoa(len(data.Array)))...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			for _, value := range data.Array {
				value, err := ParseValue(value)
				if err != nil {
					return nil, err
				}
				buf = append(buf, value...)
			}
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	case RESPTypeBulkString:
		{
			var buf []byte
			buf = append(buf, '$')
			buf = append(buf, []byte(strconv.Itoa(len(data.String)))...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			buf = append(buf, []byte(data.String)...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	case RESPTypeSimpleString:
		{
			var buf []byte
			buf = append(buf, '+')
			buf = append(buf, []byte(data.String)...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	case RESPTypeInteger:
		{
			var buf []byte
			buf = append(buf, ':')
			buf = append(buf, []byte(strconv.Itoa(data.Integer))...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	case RESPTypeNull:
		{
			var buf []byte
			buf = append(buf, '_')
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	case RESPTypeError:
		{
			var buf []byte
			buf = append(buf, '-')
			buf = append(buf, []byte(data.String)...)
			buf = append(buf, '\r')
			buf = append(buf, '\n')
			return buf, nil
		}
	default:
		return nil, fmt.Errorf("unknown type: %c and data: %s", data.Type, string(data.String))
	}
}
