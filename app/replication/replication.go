package replication

import (
	"net"
)

func SendHandshake(conn net.Conn, Config *map[string]string) {
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + (*Config)["master_port"] + "\r\n"))
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
}
