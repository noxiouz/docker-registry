package ipc

import (
	"github.com/docker/libchan"
	"io"
)

type Request struct {
	Type string
	Parameters map[string]interface{}
	ResponseChannel libchan.Sender
}

type GlorifiedReader struct {
	io.Reader
}

func (r GlorifiedReader) Write(p []byte) (n int, err error) {
	panic("Disallowed write")
}

func (r GlorifiedReader) Close() error {
	return nil
}