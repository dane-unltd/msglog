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
	nextSeq  uint64
}

type Msg struct {
	Seq    uint64
	Time   uint64
	From   uint64
	Pos    uint64
	ID     uint64
	Length uint64
}

type pack struct {
	msg  *Msg
	data []byte
}

func NewLog(filename string) (*Log, error) {
	l := &Log{filename: filename, in: make(chan pack)}

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	l.f = f
	go l.run()
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

func (l *Log) run() {
	nextSeq := uint64(0)
	pos := uint64(0)
	buffered := uint64(0)

	var intbuf [9]byte
	buf := bufio.NewWriter(l.f)

	clk := time.Tick(time.Millisecond)
	for {
		select {
		case p := <-l.in:
			if uint64(len(p.data)) < p.msg.Length {
				log.Println("not enough data")
				continue
			}

			n, _ := buf.Write(encodeUint(nextSeq, intbuf))
			pos += uint64(n)
			n, _ = buf.Write(encodeUint(uint64(time.Now().UnixNano()), intbuf))
			pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.From, intbuf))
			pos += uint64(n)
			n, _ = buf.Write(encodeUint(pos, intbuf))
			pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.ID, intbuf))
			pos += uint64(n)
			n, _ = buf.Write(encodeUint(p.msg.Length, intbuf))
			pos += uint64(n)
			n, _ = buf.Write(p.data[:p.msg.Length])
			pos += uint64(n)

			buffered++
			nextSeq++

		case <-clk:
			if buf.Buffered() > 00 {
				err := buf.Flush()
				if err != nil {
					log.Println(err)
				}
				atomic.AddUint64(&l.nextSeq, buffered)
				buffered = 0
			}
		}
	}
}

func (l *Log) Push(msg Msg, data []byte) {
	l.in <- pack{msg: &msg, data: data}
}
