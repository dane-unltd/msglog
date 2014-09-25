package msglog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

type Log struct {
	name   string
	f      *os.File
	in     chan pack
	lastID int64
}

type Msg struct {
	ID     int64
	Topic  string
	From   string
	Pos    int64
	Length int64
}

type pack struct {
	msg  *Msg
	data []byte
}

var logs = make(map[string]*Log, 10)

func NewLog(name string) (*Log, error) {
	l := &Log{name: name, in: make(chan pack), lastID: -1}

	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	l.f = f
	go l.run()
	logs[name] = l
	return l, nil
}

func (l *Log) run() {
	nextId := int64(0)
	pos := int64(0)
	for {
		p := <-l.in
		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		p.msg.ID = nextId
		p.msg.Pos = pos
		err := enc.Encode(p.msg)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("write id", p.msg.ID)

		fmt.Println(len(p.data), p.msg.Length)
		if int64(len(p.data)) < p.msg.Length {
			log.Println("not enough data")
			continue
		}

		n, err := l.f.Write(buf.Bytes())
		fmt.Println(n, err)
		if err != nil {
			log.Println(err)
			continue
		}
		pos += int64(n)
		n, err = l.f.Write(p.data[:p.msg.Length])
		fmt.Println(n, err)
		if err != nil {
			log.Println(err)
		}
		pos += int64(n)
		nextId++
		atomic.AddInt64(&l.lastID, 1)
	}
}

func (l *Log) Push(msg Msg, data []byte) {
	l.in <- pack{msg: &msg, data: data}
}
