package resp

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

type RESP byte

const (
	RESPTypeArray        RESP = '*'
	RESPTypeBulkString   RESP = '$'
	RESPTypeSimpleString RESP = '+'
	RESPTypeInteger      RESP = ':'
	RESPTypeNull         RESP = '_'
	RESPTypeError        RESP = '-'
	RESPTypeBoolean      RESP = '#'
	RESPTypeDouble       RESP = ','
	RESPTypeBigNumber    RESP = '('
)

type Value struct {
	Type      RESP
	String    string
	Integer   int
	Array     []Value
	IsNull    bool
	Boolean   bool
	Double    float64
	BigNumber int64
}

type StoreValue struct {
	Value    Value
	ExpireAt time.Time // Zero time means no expiration
}

type Database struct {
	ID        uint8
	Store     map[string]StoreValue
	ExpiryMap map[time.Time]string
}

func NewDatabase(id uint8) Database {
	return Database{
		ID:        id,
		Store:     make(map[string]StoreValue),
		ExpiryMap: make(map[time.Time]string),
	}
}

func NewBulkString(value string) Value {
	return Value{Type: RESPTypeBulkString, String: value}
}

func NewSimpleString(value string) Value {
	return Value{Type: RESPTypeSimpleString, String: value}
}

func NewStoreValue(value Value, expireAt time.Time) StoreValue {
	return StoreValue{Value: value, ExpireAt: expireAt}
}

func NewInteger(value int) Value {
	return Value{Type: RESPTypeInteger, Integer: value}
}

func NewNull() Value {
	return Value{Type: RESPTypeNull, IsNull: true}
}

func NewBoolean(value bool) Value {
	return Value{Type: RESPTypeBoolean, Boolean: value}
}

func NewDouble(value float64) Value {
	return Value{Type: RESPTypeDouble, Double: value}
}

func NewBigNumber(value int64) Value {
	return Value{Type: RESPTypeBigNumber, BigNumber: value}
}

func NewError(value string) Value {
	return Value{Type: RESPTypeError, String: value}
}

func Parse(data []byte) (Value, []byte, error) {
	switch data[0] {
	case byte(RESPTypeNull): // null
		return Value{Type: RESPTypeNull, IsNull: true}, []byte(nil), nil
	case byte(RESPTypeSimpleString): // simple string
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid basic string format: no newline")
			}
			return Value{Type: RESPTypeSimpleString, String: string(data[1 : len-1])}, data[len+1:], nil
		}
	case byte(RESPTypeInteger): // integer
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
	case byte(RESPTypeBulkString): // bulk string
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
	case byte(RESPTypeArray): // array
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
	case byte(RESPTypeError): // error
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid error format: no newline")
			}
			return Value{Type: RESPTypeError, String: string(data[1 : len-1])}, data[len+1:], nil
		}
	case byte(RESPTypeBoolean): // boolean
		{
			if bytes.Equal(data[:4], []byte("#t\r\n")) {
				return Value{Type: RESPTypeBoolean, Boolean: true}, data[4:], nil
			}
			if bytes.Equal(data[:4], []byte("#f\r\n")) {
				return Value{Type: RESPTypeBoolean, Boolean: false}, data[4:], nil
			}
			return Value{}, []byte(nil), fmt.Errorf("invalid boolean format: %s", string(data))
		}
	case byte(RESPTypeDouble): // double
		{
			i := 1
			var integral, fractional, exponent []byte
			if data[i] == '-' {
				integral = append(integral, '-')
				i++
			} else if data[i] == '+' {
				integral = append(integral, '+')
				i++
			}

			for i < len(data) && data[i] != '.' && data[i] != 'E' && data[i] != 'e' && (data[i] >= '0' && data[i] <= '9') {
				integral = append(integral, data[i])
				i++
			}
			if i < len(data) && (data[i] == '.' || data[i] == 'E' || data[i] == 'e') {
				if data[i] == '.' {
					i++
					for i < len(data) && data[i] != 'E' && data[i] != 'e' && (data[i] >= '0' && data[i] <= '9') {
						fractional = append(fractional, data[i])
						i++
					}
				}
				if i < len(data) && (data[i] == 'E' || data[i] == 'e') {
					i++
					if data[i] == '-' {
						exponent = append(exponent, '-')
						i++
					} else if data[i] == '+' {
						exponent = append(exponent, '+')
						i++
					}
					for i < len(data) && data[i] >= '0' && data[i] <= '9' {
						exponent = append(exponent, data[i])
						i++
					}
				}
			}
			i++
			if len(integral) == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid double format: no integral part")
			}
			if len(fractional) == 0 {
				fractional = []byte{'0'}
			}
			if len(exponent) == 0 {
				exponent = []byte{'0'}
			}

			double, err := strconv.ParseFloat(string(integral)+"."+string(fractional)+"e"+string(exponent), 64)
			if err != nil {
				return Value{}, []byte(nil), fmt.Errorf("invalid double format: %v", err)
			}
			return Value{Type: RESPTypeDouble, Double: double}, data[i+1:], nil
		}
	case byte(RESPTypeBigNumber): // big number
		{
			len := bytes.IndexByte(data, '\n')
			if len == -1 || len == 0 {
				return Value{}, []byte(nil), fmt.Errorf("invalid big number format: no newline")
			}
			bigNumber, err := strconv.ParseInt(string(data[1:len-1]), 10, 64)
			if err != nil {
				return Value{}, []byte(nil), fmt.Errorf("invalid big number format: %v", err)
			}
			return Value{Type: RESPTypeBigNumber, BigNumber: bigNumber}, data[len+1:], nil
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
			return buf, nil
		}
	case RESPTypeBulkString:
		return ToBulkString(data.String), nil
	case RESPTypeSimpleString:
		return ToSimpleString(data.String), nil
	case RESPTypeInteger:
		return ToInteger(data.Integer), nil
	case RESPTypeNull:
		return []byte("_\r\n"), nil
	case RESPTypeError:
		return ToError(data.String), nil
	case RESPTypeBoolean:
		return ToBoolean(data.Boolean), nil
	case RESPTypeDouble:
		return ToDouble(data.Double), nil
	case RESPTypeBigNumber:
		return ToBigNumber(data.BigNumber), nil
	default:
		return nil, fmt.Errorf("unknown type: %c and data: %s", data.Type, string(data.String))
	}
}

func ToBulkString(value string) []byte {
	if value == "" {
		return []byte("$-1\r\n")
	}
	var buf []byte
	buf = append(buf, '$')
	buf = append(buf, []byte(strconv.Itoa(len(value)))...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	buf = append(buf, []byte(value)...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToSimpleString(value string) []byte {
	if value == "" {
		return []byte("+\r\n")
	}
	var buf []byte
	buf = append(buf, '+')
	buf = append(buf, []byte(value)...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToInteger(value int) []byte {
	var buf []byte
	buf = append(buf, ':')
	buf = append(buf, []byte(strconv.Itoa(value))...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToError(value string) []byte {
	if value == "" {
		return []byte("-\r\n")
	}
	var buf []byte
	buf = append(buf, '-')
	buf = append(buf, 'E')
	buf = append(buf, 'R')
	buf = append(buf, 'R')
	buf = append(buf, ' ')
	buf = append(buf, []byte(value)...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToBoolean(value bool) []byte {
	if value {
		return []byte("#t\r\n")
	}
	return []byte("#f\r\n")
}

func ToDouble(value float64) []byte {
	var buf []byte
	buf = append(buf, ',')
	buf = append(buf, []byte(strconv.FormatFloat(value, 'e', -1, 64))...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToBigNumber(value int64) []byte {
	var buf []byte
	buf = append(buf, '(')
	buf = append(buf, []byte(strconv.FormatInt(value, 10))...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	return buf
}

func ToArray(value []any) []byte {
	var buf []byte
	buf = append(buf, '*')
	buf = append(buf, []byte(strconv.Itoa(len(value)))...)
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	for _, value := range value {
		switch v := value.(type) {
		case string:
			str := ToBulkString(v)
			buf = append(buf, str...)
		case int:
			integer := ToInteger(v)
			buf = append(buf, integer...)
		case bool:
			boolean := ToBoolean(v)
			buf = append(buf, boolean...)
		case float64:
			double := ToDouble(v)
			buf = append(buf, double...)
		case int64:
			bigNumber := ToBigNumber(v)
			buf = append(buf, bigNumber...)
		default:
			str := ToBulkString(v.(string))
			buf = append(buf, str...)
		}
	}
	return buf
}
