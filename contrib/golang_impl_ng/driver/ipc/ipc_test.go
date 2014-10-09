package ipc

import (
	"bytes"
	"math/rand"
	"os"
	"path"
	"testing"
)

var rootDirectory = "/tmp/driver"
var _ = os.RemoveAll(rootDirectory)

func setUp() (*DriverClient, error) {
	d, err := NewDriverClient("filesystem", map[string]string{"RootDirectory": rootDirectory})
	if err != nil {
		return nil, err
	}
	err = d.Start()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func tearDown(d *DriverClient) error {
	return d.Stop()
}

func runTest(t *testing.T, testFunc func(t *testing.T, d *DriverClient)) {
	d, err := setUp()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		err := tearDown(d)
		if err != nil {
			t.Fatal(err)
		}
	}()
	testFunc(t, d)
}

func TestStartClient(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {})
}

func TestWriteRead1(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {
		filename := randomPath(32)
		contents := []byte("a")
		writeReadCompare(t, d, filename, contents, contents)
	})
}

func TestWriteRead2(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {
		filename := randomPath(32)
		contents := []byte("\xc3\x9f")
		writeReadCompare(t, d, filename, contents, contents)
	})
}

func TestWriteRead3(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {
		filename := randomPath(32)
		contents := []byte(randomPath(32))
		writeReadCompare(t, d, filename, contents, contents)
	})
}

func TestRemoveExisting(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {
		filename := randomPath(32)
		contents := []byte(randomPath(32))

		err := d.PutContent(filename, contents)
		if err != nil {
			t.Error(err)
			return
		}

		err = d.Delete(filename)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = d.GetContent(filename)
		if err == nil {
			t.Errorf("%s should not exist", filename)
			return
		}
	})
}

func TestRemoveFolder(t *testing.T) {
	runTest(t, func(t *testing.T, d *DriverClient) {
		dirname := randomPath(32)
		filename1 := randomPath(32)
		filename2 := randomPath(32)
		contents := []byte(randomPath(32))

		err := d.PutContent(path.Join(dirname, filename1), contents)
		if err != nil {
			t.Error(err)
			return
		}

		err = d.PutContent(path.Join(dirname, filename2), contents)
		if err != nil {
			t.Error(err)
			return
		}

		err = d.Delete(dirname)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = d.GetContent(path.Join(dirname, filename1))
		if err == nil {
			t.Errorf("%s should not exist", path.Join(dirname, filename1))
			return
		}

		_, err = d.GetContent(path.Join(dirname, filename2))
		if err == nil {
			t.Errorf("%s should not exist", path.Join(dirname, filename2))
			return
		}
	})
}

func writeReadCompare(t *testing.T, d *DriverClient, filename string, contents, expected []byte) bool {
	err := d.PutContent(filename, contents)
	if err != nil {
		t.Error(err)
		return false
	}

	readContents, err := d.GetContent(filename)
	if err != nil {
		t.Error(err)
		return false
	}

	if !bytes.Equal(contents, readContents) {
		t.Errorf("Expected: %s, got %s", contents, readContents)
		return false
	}
	return true
}

var pathChars = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPath(length uint) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = pathChars[rand.Intn(len(pathChars))]
	}
	return string(b)
}
