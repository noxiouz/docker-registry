package inmemory

import (
	"bytes"
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
	"io"
	"io/ioutil"
)

type InMemoryDriver struct {
	storage map[string][]byte
}

func NewDriver() *InMemoryDriver {
	return &InMemoryDriver{make(map[string][]byte)}
}

func (d *InMemoryDriver) GetContent(path string) ([]byte, error) {
	contents, ok := d.storage[path]
	if !ok {
		return nil, driver.PathNotFoundError{path}
	}
	return contents, nil
}

func (d *InMemoryDriver) PutContent(path string, contents []byte) error {
	d.storage[path] = contents
	return nil
}

func (d *InMemoryDriver) ReadStream(path string) (io.ReadCloser, error) {
	contents, err := d.GetContent(path)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewReader(contents)), nil
}

func (d *InMemoryDriver) WriteStream(path string, offset uint64, reader io.ReadCloser) error {
	defer reader.Close()

	resumableOffset, err := d.ResumeWritePosition(path)
	if err != nil {
		return err
	}

	if offset > resumableOffset {
		return driver.InvalidOffsetError{path, offset}
	}

	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	d.storage[path] = contents
	return nil
}

func (d *InMemoryDriver) ResumeWritePosition(path string) (uint64, error) {
	contents, ok := d.storage[path]
	if !ok {
		return 0, nil
	}
	return uint64(len(contents)), nil
}

func (d *InMemoryDriver) Move(sourcePath string, destPath string) error {
	contents, ok := d.storage[sourcePath]
	if !ok {
		return driver.PathNotFoundError{sourcePath}
	}
	d.storage[destPath] = contents
	delete(d.storage, sourcePath)
	return nil
}

func (d *InMemoryDriver) Delete(path string) error {
	_, ok := d.storage[path]
	if !ok {
		return driver.PathNotFoundError{path}
	}
	delete(d.storage, path)
	return nil
}
