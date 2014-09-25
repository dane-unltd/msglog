package msglog

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
)

type Log struct {
	name string
	f    *os.File
	in   chan pack
	out  chan pack
}

type Msg struct {
	Topic  string
	From   string
	Length int64
}

type pack struct {
	msg  *Msg
	data []byte
}

func New(name string) (*Log, error) {
	l := &Log{name: name}

	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	l.f = f
	l.run()
	return l, nil
}

func (l *Log) run() {
	for {
		p := <-l.in
		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		err := enc.Encode(p.msg)
		if err != nil {
			log.Println(err)
			continue
		}
		_, err = l.f.Write(buf.Bytes())
		if err != nil {
			log.Println(err)
			continue
		}
		_, err = l.f.Write(p.data)
		if err != nil {
			log.Println(err)
		}
	}
}

func (l *Log) Push(msg Msg, data []byte) {
	l.in <- pack{msg: &msg, data: data}
}
