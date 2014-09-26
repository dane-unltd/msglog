package msglog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

type Consumer struct {
	l        *Log
	f        *os.File
	lastTime int64
	buf      *bufio.Reader
	data     []byte
}

func NewConsumer(name string) (*Consumer, error) {
	l, ok := logs[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("log not found: %s", name))
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &Consumer{f: f, l: l, lastTime: -1, buf: bufio.NewReader(f)}, nil
}

var errBadUint = errors.New("gob: encoded unsigned integer out of range")

func decodeInt(r io.Reader) (xi int64, width int, err error) {
	var buf [9]byte
	var x uint64
	width = 1
	n, err := io.ReadFull(r, buf[0:width])
	if n == 0 {
		return
	}
	b := buf[0]
	if b <= 0x7f {
		x = uint64(b)
		goto done
	}
	n = -int(int8(b))
	if n > uint64Size {
		err = errBadUint
		return
	}
	width, err = io.ReadFull(r, buf[0:n])
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}
	// Could check that the high byte is zero but it's not worth it.
	for _, b := range buf[0:width] {
		x = x<<8 | uint64(b)
	}
	width++ // +1 for length byte

done:
	if x&1 != 0 {
		xi = ^int64(x >> 1)
		return
	}
	xi = int64(x >> 1)
	return
}

func (c *Consumer) Next() (Msg, []byte, error) {
	for c.lastTime == atomic.LoadInt64(&c.l.lastTime) {
		time.Sleep(time.Millisecond)
	}
	msg := Msg{}
	var err error
	msg.Time, _, err = decodeInt(c.buf)
	if err != nil {
		return Msg{}, nil, err
	}
	msg.From, _, err = decodeInt(c.buf)
	if err != nil {
		return Msg{}, nil, err
	}
	msg.Pos, _, err = decodeInt(c.buf)
	if err != nil {
		return Msg{}, nil, err
	}
	msg.ID, _, err = decodeInt(c.buf)
	if err != nil {
		return Msg{}, nil, err
	}
	msg.Length, _, err = decodeInt(c.buf)
	if err != nil {
		return Msg{}, nil, err
	}

	c.lastTime = msg.Time

	if int64(len(c.data)) < msg.Length {
		c.data = make([]byte, msg.Length)
	}
	_, err = io.ReadFull(c.buf, c.data[:msg.Length])
	if err != nil {
		return Msg{}, nil, err
	}

	return msg, c.data[:msg.Length], nil
}

func (c *Consumer) Close() {
	c.f.Close()
}
