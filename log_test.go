package msglog

import (
	"testing"
)

func TestLog(t *testing.T) {
	l, err := NewLog("test")
	if err != nil {
		t.Error(err)
	}

	c1, err := l.Consumer()
	if err != nil {
		t.Error(err)
	}

	data := []byte("01234567890123456789012345678901234567890123456789")
	go func() {
		for i := 0; i < 1e6; i++ {
			l.Push(Msg{From: 1234, Length: uint64(len(data)), ID: uint64(i) + 5}, data)
		}
	}()
	for i := 0; i < 1e6; i++ {
		msg, err := c1.Next()
		if err != nil {
			t.Error(err)
		}

		if msg.ID != uint64(i)+5 {
			t.Error("wrong ID")
		}

		if i%1000 == 0 {
			rec, err := c1.Payload()
			if err != nil {
				t.Error(err)
			}
			if string(data) != string(rec) {
				t.Error("wrong data")
			}
		}
	}
}
