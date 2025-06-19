package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/crc64"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

const REDIS_VERSION = "REDIS0011"

type ValueEncoding uint8

const (
	StringEncoding ValueEncoding = iota
	ListEncoding
	SetEncoding
	SortedSetEncoding
	HashEncoding
	ZipmapEncoding
	ZiplistEncoding
	IntsetEncoding
	SortedSetInZiplistEncoding
	HashmapInZiplistEncoding
	ListInQuicklistEncoding
)

func Save(dir string, dbfilename string, metadata map[string]string, databases map[uint8]resp.Database) error {
	if dir == "" {
		dir = "./"
	}
	if dbfilename == "" {
		dbfilename = "dump.rdb"
	}

	filePath := filepath.Join(dir, dbfilename)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	buf := bytes.Buffer{}

	_, err = buf.Write([]byte(REDIS_VERSION))
	if err != nil {
		return fmt.Errorf("error writing REDIS_VERSION: %v", err)
	}

	// Start of Metadata Field
	_, err = buf.Write([]byte{0xFA})
	if err != nil {
		return fmt.Errorf("error writing metadata: %v", err)
	}
	for key, value := range metadata {
		keyBytes, err := encodeString(key)
		if err != nil {
			fmt.Println("Error encoding key: ", err.Error())
			return err
		}
		_, err = buf.Write(keyBytes)
		if err != nil {
			fmt.Println("Error writing key: ", err.Error())
			return err
		}
		valueBytes, err := encodeString(value)
		if err != nil {
			fmt.Println("Error encoding value: ", err.Error())
			return err
		}
		_, err = buf.Write(valueBytes)
		if err != nil {
			fmt.Println("Error writing value: ", err.Error())
			return err
		}
	}

	// Database Sections
	for id, database := range databases {
		_, err = buf.Write([]byte{0xFE})
		if err != nil {
			return fmt.Errorf("error writing database section: %v", err)
		}
		_, err = buf.Write([]byte{id})
		if err != nil {
			return fmt.Errorf("error writing database id: %v", err)
		}
		_, err = buf.Write([]byte{0xFB})
		if err != nil {
			return fmt.Errorf("error writing database section: %v", err)
		}
		table_size, expiry_size := 0, 0
		var KVbuf bytes.Buffer
		for key, value := range database.Store {
			if !value.ExpireAt.IsZero() {
				// Write expiry marker
				_, err := KVbuf.Write([]byte{0xFC})
				if err != nil {
					return fmt.Errorf("error writing expiry marker: %v", err)
				}

				// Write expiry time
				expiryTime := value.ExpireAt.Unix()
				err = binary.Write(&KVbuf, binary.LittleEndian, expiryTime)
				if err != nil {
					return fmt.Errorf("error writing expiry time: %v", err)
				}
				expiry_size += 1
			}
			// Write value type
			valType, err := encodeValueType(value.Value)
			if err != nil {
				return fmt.Errorf("error writing value type: %v", err)
			}
			KVbuf.WriteByte(valType)

			// Write key
			keyBytes, err := encodeString(key)
			if err != nil {
				return fmt.Errorf("error encoding key: %v", err)
			}
			_, err = KVbuf.Write(keyBytes)
			if err != nil {
				return fmt.Errorf("error writing key: %v", err)
			}

			// Write value
			valueBytes, err := encodeValue(value.Value)
			if err != nil {
				return fmt.Errorf("error encoding value: %v", err)
			}
			_, err = KVbuf.Write(valueBytes)
			if err != nil {
				return fmt.Errorf("error writing value: %v", err)
			}
			table_size += 1
		}

		// Write table size
		table_size_bytes, err := encodeLength(table_size)
		if err != nil {
			return fmt.Errorf("error encoding table size: %v", err)
		}
		if _, err = buf.Write(table_size_bytes); err != nil {
			return fmt.Errorf("error writing table size: %v", err)
		}

		// Write expiry size
		expiry_size_bytes, err := encodeLength(expiry_size)
		if err != nil {
			return fmt.Errorf("error encoding expiry size: %v", err)
		}
		if _, err = buf.Write(expiry_size_bytes); err != nil {
			return fmt.Errorf("error writing expiry size: %v", err)
		}

		// Write key, values
		_, err = buf.Write(KVbuf.Bytes())
		if err != nil {
			return fmt.Errorf("error writing key, values to database: %v", err)
		}

	}

	// Write end of database marker
	if _, err = buf.Write([]byte{0xFF}); err != nil {
		return fmt.Errorf("error writing end of database: %v", err)
	}

	checksum := crc64.Digest(buf.Bytes())
	checksumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)

	if _, err = file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("error writing buffer to file: %v", err)
	}

	if _, err = file.Write(checksumBytes); err != nil {
		return fmt.Errorf("error writing checksum: %v", err)
	}
	return nil
}

func Open(dir string, dbfilename string) (metadata map[string]string, databases map[uint8]resp.Database, err error) {
	if dir == "" {
		dir = "./"
	}
	if dbfilename == "" {
		dbfilename = "dump.rdb"
	}

	filePath := filepath.Join(dir, dbfilename)
	// fmt.Printf("Opening file: %v\n", filePath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("file does not exist: %v", filePath)
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading file: %v", err)
	}
	// fmt.Printf("File bytes: % X\n", fileBytes)

	if len(fileBytes) < 8 {
		return nil, nil, fmt.Errorf("file is too small: % X", fileBytes)
	}

	// Checksum
	index := len(fileBytes) - 1
	for ; index >= 0; index-- {
		if fileBytes[index] == 0xFF {
			break
		}
	}
	checksum := crc64.Digest(fileBytes[:index+1])
	if checksum != binary.LittleEndian.Uint64(fileBytes[index+1:]) {
		return nil, nil, fmt.Errorf("invalid checksum: %v", checksum)
	}
	fileBytes = fileBytes[:index+1]

	// Redis Version
	redis_version := []byte(REDIS_VERSION)
	if !bytes.Equal(fileBytes[:len(redis_version)], redis_version) {
		return nil, nil, fmt.Errorf("invalid rdb version: %v", string(fileBytes[:len(redis_version)]))
	}
	fileBytes = fileBytes[len(redis_version):]
	metadata = make(map[string]string)
	databases = make(map[uint8]resp.Database)

	// Metadata Section
	for fileBytes[0] == 0xFA {
		fileBytes = fileBytes[1:]
		key, err := decodeString(&fileBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid metadata: %v", err)
		}
		value, err := decodeString(&fileBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid metadata: %v", err)
		}
		metadata[key] = value
	}

	// Database Sections
	for len(fileBytes) > 0 && (fileBytes[0] == 0xFE || fileBytes[0] == 0xFF) {
		if fileBytes[0] == 0xFF {
			break
		}
		fileBytes = fileBytes[1:]

		databaseId := fileBytes[0]
		fileBytes = fileBytes[1:]

		databases[databaseId] = resp.NewDatabase(databaseId)

		if fileBytes[0] != 0xFB {
			return nil, nil, fmt.Errorf("invalid database section: %v", fileBytes[0])
		}
		fileBytes = fileBytes[1:]

		tableSize, err := decodeLength(&fileBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid table size: %v", err)
		}

		expirySize, err := decodeLength(&fileBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid expiry size: %v", err)
		}

		// fmt.Printf("tableSize: %v, expirySize: %v\n", tableSize, expirySize)

		for tableSize > 0 || expirySize > 0 {
			expiryTime := time.Time{}
			if fileBytes[0] == 0xFD {
				expirySize -= 1
				fileBytes = fileBytes[1:]
				sec_time, err := decodeTimeStamp(&fileBytes, 4)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid expiry time: %v", err)
				}
				// fmt.Printf("expiryTime: %s\n", sec_time)
				expiryTime = sec_time
			} else if fileBytes[0] == 0xFC {
				expirySize -= 1
				fileBytes = fileBytes[1:]
				sec_time, err := decodeTimeStamp(&fileBytes, 8)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid expiry time: %v", err)
				}
				// fmt.Printf("expiryTime: %s\n", sec_time)
				expiryTime = sec_time
			}

			valueType := fileBytes[0]
			fileBytes = fileBytes[1:]

			// fmt.Printf("valueType: %v\n", valueType)
			// fmt.Printf("fileBytes: % X\n", fileBytes)
			key, err := decodeString(&fileBytes)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid key: %v", err)
			}
			// fmt.Printf("key: %v\n", key)

			switch valueType {
			case 0x00:
				value, err := decodeValue(&fileBytes)
				// fmt.Printf("value: %s\n", value)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid value: %v", err)
				}
				if expiryTime != (time.Time{}) && time.Now().Before(expiryTime) {
					databases[databaseId].Store[key] = resp.NewStoreValue(resp.NewBulkString(""), expiryTime)
				} else {
					databases[databaseId].Store[key] = resp.NewStoreValue(value, expiryTime)
					if expiryTime != (time.Time{}) {
						databases[databaseId].ExpiryMap[expiryTime] = key
					}
				}
				tableSize -= 1
			default:
				return nil, nil, fmt.Errorf("invalid value type: %v", valueType)
			}
		}

		if tableSize != 0 || expirySize != 0 {
			return nil, nil, fmt.Errorf("invalid table size: %v, expiry size: %v", tableSize, expirySize)
		}
	}
	// fmt.Println("databases: ", databases)

	// End of Database Sections
	if fileBytes[0] != 0xFF || len(fileBytes) != 1 {
		return nil, nil, fmt.Errorf("invalid end of database sections: % X", fileBytes)
	}

	return metadata, databases, nil
}

func encodeLength(length int) ([]byte, error) {
	var buf bytes.Buffer

	if length < 64 {
		// 00: Next 6 bits = length (0-63)
		buf.WriteByte(byte(length))
	} else if length < 16384 {
		// 01: Next 14 bits = length (0-16383)
		// First byte: 01 + 6 bits of length
		upper := byte((length&0x3F)<<2) | 0x40
		buf.WriteByte(upper)
		// Second byte: remaining 8 bits
		buf.WriteByte(byte(length >> 8))
	} else {
		// 10: Next 4 bytes = length (0-2^32-1)
		buf.WriteByte(0x80) // 10 + 000000
		err := binary.Write(&buf, binary.LittleEndian, int32(length))
		if err != nil {
			return nil, fmt.Errorf("error writing length: %v", err)
		}
	}
	return buf.Bytes(), nil
}

func decodeLength(data *[]byte) (int, error) {
	if len(*data) == 0 {
		return 0, fmt.Errorf("empty data")
	}

	firstByte := (*data)[0]
	*data = (*data)[1:]
	if (firstByte & 0xC0) == 0x00 {
		return int(firstByte), nil
	} else if (firstByte & 0xC0) == 0x40 {
		secondByte := (*data)[0]
		*data = (*data)[1:]
		return int(firstByte&0x3F)<<8 | int(secondByte), nil
	} else if (firstByte & 0xC0) == 0x80 {
		var length int32
		binary.Read(bytes.NewReader((*data)[:4]), binary.LittleEndian, &length)
		*data = (*data)[4:]
		return int(length), nil
	} else {
		return 0, fmt.Errorf("invalid length prefix: %v", firstByte)
	}
}

func encodeString(s string) ([]byte, error) {
	lengthBytes, err := encodeLength(len(s))
	if err != nil {
		return nil, fmt.Errorf("error encoding length prefix: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(lengthBytes)
	buf.WriteString(s)
	return buf.Bytes(), nil
}

func decodeString(data *[]byte) (string, error) {
	if len(*data) == 0 {
		return "", fmt.Errorf("empty data")
	}

	// Decode Integer
	firstByte := (*data)[0]
	if firstByte == 0xC0 {
		var value int8
		*data = (*data)[1:]
		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
		*data = (*data)[1:]
		return strconv.FormatInt(int64(value), 10), nil
	} else if firstByte == 0xC1 {
		var value int16
		*data = (*data)[1:]
		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
		*data = (*data)[2:]
		return strconv.FormatInt(int64(value), 10), nil
	} else if firstByte == 0xC2 {
		var value int32
		*data = (*data)[1:]
		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
		*data = (*data)[4:]
		return strconv.FormatInt(int64(value), 10), nil
	}

	// Decode String
	length, err := decodeLength(data)
	if err != nil {
		return "", fmt.Errorf("error decoding length prefix: %v", err)
	}

	str := string((*data)[:length])
	*data = (*data)[length:]
	return str, nil
}

func encodeInteger[N uint8 | uint16 | uint32 | int8 | int16 | int32 | int](value N) ([]byte, error) {
	var buf bytes.Buffer

	// Special format: 11 + type indicator
	// For integers: 000000 = 8-bit, 000001 = 16-bit, 000010 = 32-bit
	var typeIndicator byte

	valueInt := int(value)
	if valueInt < int(256) {
		typeIndicator = 0xC0
	} else if valueInt < int(65536) {
		typeIndicator = 0xC1
	} else {
		typeIndicator = 0xC2
	}

	buf.WriteByte(typeIndicator) // 11 + type indicator

	switch typeIndicator {
	case 0xC0:
		err := binary.Write(&buf, binary.LittleEndian, int8(valueInt))
		if err != nil {
			return nil, fmt.Errorf("error writing integer: %v", err)
		}
	case 0xC1:
		err := binary.Write(&buf, binary.LittleEndian, int16(valueInt))
		if err != nil {
			return nil, fmt.Errorf("error writing integer: %v", err)
		}
	case 0xC2:
		err := binary.Write(&buf, binary.LittleEndian, int32(valueInt))
		if err != nil {
			return nil, fmt.Errorf("error writing integer: %v", err)
		}
	}

	return buf.Bytes(), nil
}

func decodeTimeStamp(data *[]byte, byteCount int) (time.Time, error) {
	if len(*data) == 0 {
		return time.Time{}, fmt.Errorf("empty data")
	}
	if byteCount == 4 {
		var value int32
		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
		*data = (*data)[4:]
		sec := int64(value / 1000)
		nsec := (int64(value%1000) * int64(time.Millisecond))
		return time.Unix(sec, nsec), nil
	}
	if byteCount == 8 {
		var value int64
		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
		*data = (*data)[8:]
		sec := value / 1000
		nsec := (value % 1000) * int64(time.Millisecond)
		return time.Unix(sec, nsec), nil
	}
	return time.Time{}, fmt.Errorf("invalid byte count: %v", byteCount)
}

// func decodeInteger(data *[]byte) (int, error) {
// 	if len(*data) == 0 {
// 		return 0, fmt.Errorf("empty data")
// 	}

// 	firstByte := (*data)[0]
// 	*data = (*data)[1:]
// 	if firstByte == 0xC0 {
// 		var value int8
// 		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
// 		*data = (*data)[1:]
// 		return int(value), nil
// 	} else if firstByte == 0xC1 {
// 		var value int16
// 		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
// 		*data = (*data)[2:]
// 		return int(value), nil
// 	} else if firstByte == 0xC2 {
// 		var value int32
// 		binary.Read(bytes.NewReader(*data), binary.LittleEndian, &value)
// 		*data = (*data)[4:]
// 		return int(value), nil
// 	} else {
// 		return 0, fmt.Errorf("invalid integer prefix")
// 	}
// }

func encodeValue(value resp.Value) ([]byte, error) {
	switch value.Type {
	// case resp.RESPTypeArray:
	// 	return encodeArray(value)
	case resp.RESPTypeSimpleString, resp.RESPTypeBulkString:
		return encodeString(value.String)
	case resp.RESPTypeInteger:
		return encodeInteger(value.Integer)
	case resp.RESPTypeNull:
		return encodeString("_\r\n")
	case resp.RESPTypeError:
		return encodeString(value.String)
	case resp.RESPTypeBoolean:
		if value.Boolean {
			return []byte{0xFE}, nil
		}
		return []byte{0xFD}, nil
	case resp.RESPTypeDouble:
		str := strconv.FormatFloat(value.Double, 'g', 17, 64) // 'g' format with 17 digits of precision
		return encodeString(str)
	case resp.RESPTypeBigNumber:
		str := strconv.FormatInt(value.BigNumber, 10)
		return encodeString(str)
	default:
		return nil, fmt.Errorf("unknown type: %c", value.Type)
	}
}

func decodeValue(data *[]byte) (resp.Value, error) {
	if len(*data) == 0 {
		return resp.Value{}, fmt.Errorf("empty data")
	}

	str, err := decodeString(data)
	if err != nil {
		return resp.Value{}, err
	}

	if integer, err := strconv.Atoi(str); err == nil {
		return resp.NewInteger(integer), nil
	}

	if double, err := strconv.ParseFloat(str, 64); err == nil {
		return resp.NewDouble(double), nil
	}

	if bigNumber, err := strconv.ParseInt(str, 10, 64); err == nil {
		return resp.NewBigNumber(bigNumber), nil
	}

	if str == "1" {
		return resp.NewBoolean(true), nil
	}

	if str == "0" {
		return resp.NewBoolean(false), nil
	}

	return resp.NewBulkString(str), nil
}

func encodeValueType(value resp.Value) (byte, error) {
	switch value.Type {
	case resp.RESPTypeSimpleString, resp.RESPTypeBulkString, resp.RESPTypeInteger, resp.RESPTypeNull, resp.RESPTypeError, resp.RESPTypeBoolean, resp.RESPTypeDouble, resp.RESPTypeBigNumber:
		return byte(StringEncoding), nil
	default:
		return byte(StringEncoding), nil
	}
}

// func decodeValueType(valueType byte) (resp.ValueType, error) {
// 	switch valueType {
// 	case 0xC0, 0xC1, 0xC2:
// 		return resp.RESPTypeSimpleString, nil
// 	default:
// 		return resp.RESPTypeSimpleString, nil
// 	}
// }
