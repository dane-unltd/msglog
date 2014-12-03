package msglog

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"time"
)

type Consumer struct {
	l       *Log
	f       *os.File
	nextSeq uint64
	buf     *bufio.Reader
	data    []byte
	payload uint64
	current Msg
	abort   chan struct{}
}

func (l *Log) Consumer() (*Consumer, error) {
	f, err := os.Open(l.filename)
	if err != nil {
		return nil, err
	}
	return &Consumer{f: f, l: l, buf: bufio.NewReader(f), abort: make(chan struct{}, 1)}, nil
}

var errBadUint = errors.New("gob: encoded unsigned integer out of range")

func decodeInt(r *bufio.Reader) (x uint64, width int, err error) {
	width = 1
	var b byte
	var n int
	b, err = r.ReadByte()
	if err != nil {
		return
	}
	if b <= 0x7f {
		x = uint64(b)
		return
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
	return
}

func (c *Consumer) HasNext() bool {
	return c.nextSeq != atomic.LoadUint64(&c.l.nextSeq)
}

func (c *Consumer) Next() (msg Msg, err error) {
	if c.payload > 0 {
		n := uint64(c.buf.Buffered())
		if n > c.payload {
			_, err = c.Payload()
			if err != nil {
				return
			}
		} else {
			c.payload -= n
			_, err = c.f.Seek(int64(c.payload), os.SEEK_CUR)
			if err != nil {
				return
			}
			c.buf.Reset(c.f)
			c.payload = 0
		}
	}
	for c.nextSeq == atomic.LoadUint64(&c.l.nextSeq) {
		select {
		case <-abort:
			err = errors.New("Consumer: Closed.")
			return
		default:
		}
		time.Sleep(time.Millisecond)
	}
	msg.Seq, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.Time, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.From, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.Pos, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.PrevPos, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.ID, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}
	msg.Length, _, err = decodeInt(c.buf)
	if err != nil {
		return
	}

	c.nextSeq = msg.Seq + 1
	c.payload = msg.Length
	c.current = msg
	return
}

func (c *Consumer) Payload() (pl []byte, err error) {
	if c.payload == 0 {
		return
	}

	if uint64(len(c.data)) < c.payload {
		c.data = make([]byte, c.payload)
	}
	pl = c.data[:c.payload]
	_, err = io.ReadFull(c.buf, pl)
	c.payload = 0
	return
}

func (c *Consumer) Read(buf []byte) (int, error) {
	pl, err := c.Payload()
	copy(buf, pl)

	if len(pl) < len(buf) {
		return len(pl)
	}
	return len(buf)
}

func (c *Consumer) Goto(seq uint64) error {
	c.nextSeq = seq
	if seq == 0 {
		c.buf.Reset(c.f)
		c.payload = 0
		_, err := c.f.Seek(0, os.SEEK_SET)
		if err != nil {
			return err
		}
		return nil
	}
	if seq == c.current.Seq {
		c.buf.Reset(c.f)
		c.payload = 0
		_, err := c.f.Seek(int64(c.current.Pos), os.SEEK_SET)
		c.nextSeq = c.current.Seq
		if err != nil {
			return err
		}
		return nil
	}
	for seq > c.current.Seq+1 {
		_, err := c.Next()
		if err != nil {
			return err
		}
	}
	for seq <= c.current.Seq {
		c.buf.Reset(c.f)
		c.payload = 0
		_, err := c.f.Seek(int64(c.current.PrevPos), os.SEEK_SET)
		c.nextSeq = c.current.Seq - 1
		if err != nil {
			return err
		}
		_, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) Close() {
	c.abort <- struct{}{}
	c.f.Close()
}
