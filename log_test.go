package msglog

import (
	"testing"
)

func TestLog(t *testing.T) {
	l, err := NewLog("test")
	if err != nil {
		t.Error(err)
	}

	c1, err := NewConsumer("test")
	if err != nil {
		t.Error(err)
	}

	go func() {
		for i := 0; i < 1e6; i++ {
			l.Push(Msg{From: 1234, Length: 5, ID: int64(i) + 5}, []byte("hello"))
		}
	}()
	for i := 0; i < 1e6; i++ {
		msg, data, err := c1.Next()
		if err != nil {
			t.Error(err)
		}

		if msg.ID != int64(i)+5 {
			t.Error("wrong ID")
		}
		if string(data) != "hello" {
			t.Error("wrong data")
		}
	}
}

func BenchmarkLog(b *testing.B) {
	l, err := NewLog("bench")
	if err != nil {
		b.Error(err)
	}

	c1, err := NewConsumer("bench")
	if err != nil {
		b.Error(err)
	}
	go func() {
		for i := 0; i < 1e6; i++ {
			l.Push(Msg{From: 1234, Length: 5, ID: int64(i) + 5}, []byte("hello"))
		}
	}()
	for i := 0; i < b.N; i++ {
		msg, data, err := c1.Next()
		if err != nil {
			b.Error(err)
		}

		if msg.ID != 5 {
			b.Error("wrong ID")
		}
		if string(data) != "hello" {
			b.Error("wrong data")
		}
	}
}
