package tarantool

import (
	"net"
	"time"
)

type DeadlineIO struct {
	to time.Duration
	c  net.Conn
}

func (d *DeadlineIO) Write(b []byte) (n int, err error) {
	if d.to > 0 {
		d.c.SetWriteDeadline(time.Now().Add(d.to))
	}
	n, err = d.c.Write(b)
	return
}

func (d *DeadlineIO) Read(b []byte) (n int, err error) {
	if d.to > 0 {
		d.c.SetReadDeadline(time.Now().Add(d.to))
	}
	n, err = d.c.Read(b)
	return
}
