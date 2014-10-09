package driver

import (
	"bytes"
	"io"
	"io/ioutil"
)

type InMemoryDriver struct {
	storage map[string][]byte
}

func NewInMemoryDriver() *InMemoryDriver {
	return &InMemoryDriver{make(map[string][]byte)}
}

func (d *InMemoryDriver) GetContent(path string) ([]byte, error) {
	contents, ok := d.storage[path]
	if !ok {
		return nil, PathNotFoundError{path}
	}
	return contents, nil
}

func (d *InMemoryDriver) PutContent(path string, contents []byte) error {
	d.storage[path] = contents
	return nil
}

func (d *InMemoryDriver) ReadStream(path string) (io.Reader, error) {
	contents, err := d.GetContent(path)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(contents), nil
}

func (d *InMemoryDriver) WriteStream(path string, reader io.Reader) error {
	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	d.storage[path] = contents
	return nil
}

func (d *InMemoryDriver) Move(sourcePath string, destPath string) error {
	contents, ok := d.storage[sourcePath]
	if !ok {
		return PathNotFoundError{sourcePath}
	}
	d.storage[destPath] = contents
	delete(d.storage, sourcePath)
	return nil
}

func (d *InMemoryDriver) Delete(path string) error {
	_, ok := d.storage[path]
	if !ok {
		return PathNotFoundError{path}
	}
	delete(d.storage, path)
	return nil
}