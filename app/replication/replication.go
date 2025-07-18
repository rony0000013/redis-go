package replication

import (
	"net"
)

func SendHandshake(conn net.Conn) {
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	// conn.Write(resp.ToSimpleString("*2\r\n$11\r\nREPLCONF\r\n$11\r\nSETNAME\r\n"))
}
