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

func decodeInt(r *bufio.Reader) (xi int64, width int, err error) {
	var x uint64
	width = 1
	var b byte
	var n int
	b, err = r.ReadByte()
	if err != nil {
		return
	}
	if b <= 0x7f {
		x = uint64(b)
		goto done
	}
	n = -int(int8(b))
	if n > uint64Size {
		err = errBadUint
		return
	}
	// Could check that the high byte is zero but it's not worth it.
	for i := 0; i < n; i++ {
		b, err = r.ReadByte()
		if err != nil {
			return
		}
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
