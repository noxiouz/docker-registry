package ipc

import (
	"errors"
	"github.com/docker/libchan"
	"io"
)

type Request struct {
	Type string
	Parameters map[string]interface{}
	ResponseChannel libchan.Sender
}

type noWriteReadWriteCloser struct {
	io.ReadCloser
}

func (r noWriteReadWriteCloser) Write(p []byte) (n int, err error) {
	return 0, errors.New("Write unsupported")
}

func WrapReadCloser(readCloser io.ReadCloser) io.ReadWriteCloser {
	return noWriteReadWriteCloser{readCloser}
}
