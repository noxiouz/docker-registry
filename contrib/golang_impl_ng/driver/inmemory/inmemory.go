package inmemory

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
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

func (d *InMemoryDriver) ReadStream(path string, offset uint64) (io.ReadCloser, error) {
	contents, err := d.GetContent(path)
	if err != nil {
		return nil, err
	} else if len(contents) < int(offset) {
		return nil, driver.InvalidOffsetError{path, offset}
	}

	return ioutil.NopCloser(bytes.NewReader(contents[offset:])), nil
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

	if offset > 0 {
		contents = append(d.storage[path][0:offset], contents...)
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

func (d *InMemoryDriver) List(prefix string) ([]string, error) {
	subPathMatcher, err := regexp.Compile(fmt.Sprintf("^%s/[^/]+", prefix))
	if err != nil {
		return nil, err
	}

	keySet := make(map[string]bool)
	for k := range d.storage {
		if key := subPathMatcher.FindString(k); key != "" {
			keySet[key] = true
		}
	}

	i := 0
	keys := make([]string, len(keySet))
	for k := range keySet {
		keys[i] = k
		i++
	}
	return keys, nil
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
	subPaths := make([]string, 0)
	for k := range d.storage {
		if strings.HasPrefix(k, path) {
			subPaths = append(subPaths, k)
		}
	}

	if len(subPaths) == 0 {
		return driver.PathNotFoundError{path}
	}

	for _, subPath := range subPaths {
		delete(d.storage, subPath)
	}
	return nil
}
