package inmemory

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"sync"

	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
)

type InMemoryDriver struct {
	storage map[string][]byte
	mutex   sync.RWMutex
}

func NewDriver() *InMemoryDriver {
	return &InMemoryDriver{storage: make(map[string][]byte)}
}

func (d *InMemoryDriver) GetContent(path string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	contents, ok := d.storage[path]
	if !ok {
		return nil, driver.PathNotFoundError{path}
	}
	return contents, nil
}

func (d *InMemoryDriver) PutContent(path string, contents []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.storage[path] = contents
	return nil
}

func (d *InMemoryDriver) ReadStream(path string, offset uint64) (io.ReadCloser, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	contents, err := d.GetContent(path)
	if err != nil {
		return nil, err
	} else if len(contents) < int(offset) {
		return nil, driver.InvalidOffsetError{path, offset}
	}

	src := contents[offset:]
	buf := make([]byte, len(src))
	copy(buf, src)
	return ioutil.NopCloser(bytes.NewReader(buf)), nil
}

func (d *InMemoryDriver) WriteStream(path string, offset uint64, reader io.ReadCloser) error {
	defer reader.Close()
	d.mutex.RLock()
	defer d.mutex.RUnlock()

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
	d.mutex.RLock()
	defer d.mutex.RUnlock()
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

	d.mutex.RLock()
	defer d.mutex.RUnlock()
	// we use map to collect uniq keys
	keySet := make(map[string]struct{})
	for k := range d.storage {
		if key := subPathMatcher.FindString(k); key != "" {
			keySet[key] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	return keys, nil
}

func (d *InMemoryDriver) Move(sourcePath string, destPath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	contents, ok := d.storage[sourcePath]
	if !ok {
		return driver.PathNotFoundError{sourcePath}
	}
	d.storage[destPath] = contents
	delete(d.storage, sourcePath)
	return nil
}

func (d *InMemoryDriver) Delete(path string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
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
