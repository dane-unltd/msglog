package msglog

import (
	"bufio"
	"log"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

type Log struct {
	filename string
	f        *os.File
	in       chan pack
	quit     chan chan struct{}
	nextSeq  uint64
	pos      uint64
}

type Msg struct {
	Seq     uint64
	Time    uint64
	From    uint64
	Pos     uint64
	PrevPos uint64
	ID      uint64
	Length  uint64
}

func (m Msg) TotalSize() uint64 {

	var intbuf [9]byte
	var size uint64

	size += uint64(len(encodeUint(m.Seq, intbuf)))
	size += uint64(len(encodeUint(m.Time, intbuf)))
	size += uint64(len(encodeUint(m.From, intbuf)))
	size += uint64(len(encodeUint(m.Pos, intbuf)))
	size += uint64(len(encodeUint(m.PrevPos, intbuf)))
	size += uint64(len(encodeUint(m.ID, intbuf)))
	size += uint64(len(encodeUint(m.Length, intbuf)))
	size += m.Length

	return size
}

type pack struct {
	msg  *Msg
	data []byte
}

func New(filename string) (*Log, error) {
	l := &Log{filename: filename, in: make(chan pack), quit: make(chan chan struct{})}

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	l.f = f
	go l.run(0)
	return l, nil
}

func Recover(filename string) (*Log, error) {
	f, err := os.Open(filename)
	if err != nil {
		l, err := New(filename)
		if err != nil {
			return nil, err
		}
		return l, nil
	}

	l := &Log{
		filename: filename,
		f:        f,
		in:       make(chan pack),
		quit:     make(chan chan struct{}),
		nextSeq:  0xFFFFFFFFFFFFFFFF,
	}
	c := &Consumer{f: f, l: l, buf: bufio.NewReader(f)}

	var lastMsg Msg
	first := true
	for {
		msg, err := c.Next()
		if err != nil {
			break
		}
		_, err = c.Payload()
		if err != nil {
			break
		}

		first = false
		lastMsg = msg
	}

	f.Close()

	var size uint64
	if !first {
		size = lastMsg.Pos + lastMsg.TotalSize()
	}
	err = os.Truncate(filename, int64(size))
	if err != nil {
		return nil, err
	}

	f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}

	l.pos = size
	l.nextSeq = lastMsg.Seq + 1
	l.f = f

	go l.run(lastMsg.Pos)

	return l, nil
}

const uint64Size = int(unsafe.Sizeof(uint64(0)))

func encodeUint(x uint64, buf [9]byte) []byte {
	if x <= 0x7F {
		buf[0] = uint8(x)
		return buf[0:1]
	}
	i := uint64Size
	for x > 0 {
		buf[i] = uint8(x)
		x >>= 8
		i--
	}
	buf[i] = uint8(i - uint64Size) // = loop count, negated
	return buf[i : uint64Size+1]
}

func (l *Log) run(currPos uint64) {
	defer l.f.Close()
	nextSeq := l.nextSeq
	buffered := uint64(0)

	var intbuf [9]byte
	buf := bufio.NewWriter(l.f)

	clk := time.Tick(time.Millisecond)

	var prevPos uint64
	for {
		select {
		case p := <-l.in:
			if p.msg.Length == 0 {
				p.msg.Length = uint64(len(p.data))
			}
			if uint64(len(p.data)) < p.msg.Length {
				log.Println("not enough data")
				continue
			}

			prevPos = currPos
			currPos = l.pos

			n, _ := buf.Write(encodeUint(nextSeq, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(uint64(time.Now().UnixNano()), intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.From, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(currPos, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(prevPos, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.ID, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.Length, intbuf))
			l.pos += uint64(n)
			n, _ = buf.Write(p.data[:p.msg.Length])
			l.pos += uint64(n)

			buffered++
			nextSeq++

		case <-clk:
			if buf.Buffered() > 0 {
				err := buf.Flush()
				if err != nil {
					log.Println("msglog write error:", err)
				}
				atomic.AddUint64(&l.nextSeq, buffered)
				buffered = 0
			}
		case ret := <-l.quit:
			err := buf.Flush()
			if err != nil {
				log.Println("msglog write error:", err)
			}
			ret <- struct{}{}
			return
		}
	}
}

func (l *Log) Push(msg Msg, data []byte) {
	l.in <- pack{msg: &msg, data: data}
}

func (l *Log) Close() {
	ret := make(chan struct{})
	l.quit <- ret
	<-ret
}
