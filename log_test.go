package msglog

import (
	"fmt"
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

	l.Push(Msg{Topic: "test", From: "me", Length: 5}, []byte("hello"))
	msg, data, err := c1.Next()

	if msg.Topic != "test" {
		t.Error("wrong topic")
	}

	fmt.Println(msg, data, err)
}
