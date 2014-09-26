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
	name     string
	f        *os.File
	in       chan pack
	lastTime int64
}

type Msg struct {
	Time   int64
	From   int64
	Pos    int64
	ID     int64
	Length int64
}

type pack struct {
	msg  *Msg
	data []byte
}

var logs = make(map[string]*Log, 10)

func NewLog(name string) (*Log, error) {
	l := &Log{name: name, in: make(chan pack), lastTime: -1}

	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	l.f = f
	go l.run()
	logs[name] = l
	return l, nil
}

const uint64Size = int(unsafe.Sizeof(uint64(0)))

func encodeInt(xi int64, buf [9]byte) []byte {
	var x uint64
	if xi < 0 {
		x = uint64(^xi<<1) | 1
	} else {
		x = uint64(xi << 1)
	}
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

func (l *Log) run() {
	nextTime := int64(0)
	pos := int64(0)
	buffered := int64(0)

	var intbuf [9]byte
	buf := bufio.NewWriter(l.f)

	clk := time.Tick(time.Millisecond)
	for {
		select {
		case p := <-l.in:
			if int64(len(p.data)) < p.msg.Length {
				log.Println("not enough data")
				continue
			}

			n, _ := buf.Write(encodeInt(nextTime, intbuf))
			pos += int64(n)
			n, _ = buf.Write(encodeInt(p.msg.From, intbuf))
			pos += int64(n)
			n, _ = buf.Write(encodeInt(pos, intbuf))
			pos += int64(n)
			n, _ = buf.Write(encodeInt(p.msg.ID, intbuf))
			pos += int64(n)
			n, _ = buf.Write(encodeInt(p.msg.Length, intbuf))
			pos += int64(n)
			n, _ = buf.Write(p.data[:p.msg.Length])
			pos += int64(n)

			buffered++
			nextTime++

		case <-clk:
			if buf.Buffered() > 00 {
				err := buf.Flush()
				if err != nil {
					log.Println(err)
				}
				atomic.AddInt64(&l.lastTime, buffered)
				buffered = 0
			}
		}
	}
}

func (l *Log) Push(msg Msg, data []byte) {
	l.in <- pack{msg: &msg, data: data}
}
