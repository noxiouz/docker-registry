package driver

import (
	"bytes"
	"math/rand"
	"os"
	"path"
	"testing"
)

var rootDirectory = "/tmp/driver"
var _ = os.RemoveAll(rootDirectory)
var d = NewFilesystemDriver(rootDirectory)

func TestWriteRead1(t *testing.T) {
	filename := randomPath(32)
	contents := []byte("a")
	writeReadCompare(t, filename, contents, contents)
}

func TestWriteRead2(t *testing.T) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	writeReadCompare(t, filename, contents, contents)
}

func TestWriteRead3(t *testing.T) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024 * 1024))
	writeReadCompare(t, filename, contents, contents)
}

func TestRemoveExisting(t *testing.T) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024))

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
}

func TestRemoveFolder(t *testing.T) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	contents := []byte(randomPath(1024))

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
}

func writeReadCompare(t *testing.T, filename string, contents, expected []byte) bool {
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
