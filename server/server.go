package server

import (
	"github.com/dane-unltd/msglog"
)

type Server struct {
	l *msglog.Log
}

func New(l *msglog.Log) *Server {
	s := &Server{l: l}
	return s
}

func (s *Server) Listen(addr string) {
}
