package msglog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Consumer struct {
	l      *Log
	f      *os.File
	lastID int64
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
	return &Consumer{f: f, l: l, lastID: -1}, nil
}

func (c *Consumer) Next() (Msg, []byte, error) {
	for c.lastID == atomic.LoadInt64(&c.l.lastID) {
		time.Sleep(time.Millisecond)
	}
	fmt.Println(c.lastID, c.l.lastID)
	dec := json.NewDecoder(c.f)
	msg := Msg{}
	err := dec.Decode(&msg)
	if err != nil {
		return Msg{}, nil, err
	}
	fmt.Println(msg.ID)

	c.lastID = msg.ID

	buf := make([]byte, msg.Length)
	_, err = c.f.Read(buf)
	if err != nil {
		return Msg{}, nil, err
	}

	return msg, buf, nil
}

func (c *Consumer) Close() {
	c.f.Close()
}
