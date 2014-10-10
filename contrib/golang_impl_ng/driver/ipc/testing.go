package ipc

import (
	"bytes"
	"math/rand"
	"path"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner
func Test(t *testing.T) { TestingT(t) }

type IPCDriverSuite struct {
	*TestDriverConfig
	*DriverClient
}

type TestDriverConfig struct {
	Name   string
	Params map[string]string
}

func (suite *IPCDriverSuite) SetUpSuite(c *C) {
	d, err := NewDriverClient(suite.TestDriverConfig.Name, suite.TestDriverConfig.Params)
	if err != nil {
		c.Fatal(err)
	}
	err = d.Start()
	if err != nil {
		c.Fatal(err)
	}
	suite.DriverClient = d
}

func (suite *IPCDriverSuite) TearDownSuite(c *C) {
	if err := suite.DriverClient.Stop(); err != nil {
		c.Fatal(err)
	}
}

func (suite *IPCDriverSuite) TestStartClient(c *C) {
	// Do nothing. This just tests the ipc start/stop
}

func (suite *IPCDriverSuite) TestWriteRead1(c *C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestWriteRead2(c *C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestWriteRead3(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestRemoveExisting(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.DriverClient.PutContent(filename, contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.DriverClient.Delete(filename)
	if err != nil {
		c.Error(err)
		return
	}

	_, err = suite.DriverClient.GetContent(filename)
	if err == nil {
		c.Errorf("%s should not exist", filename)
		return
	}
}

func (suite *IPCDriverSuite) TestRemoveFolder(c *C) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.DriverClient.PutContent(path.Join(dirname, filename1), contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.DriverClient.PutContent(path.Join(dirname, filename2), contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.DriverClient.Delete(dirname)
	if err != nil {
		c.Error(err)
		return
	}

	_, err = suite.DriverClient.GetContent(path.Join(dirname, filename1))
	if err == nil {
		c.Errorf("%s should not exist", path.Join(dirname, filename1))
		return
	}

	_, err = suite.DriverClient.GetContent(path.Join(dirname, filename2))
	if err == nil {
		c.Errorf("%s should not exist", path.Join(dirname, filename2))
		return
	}
}

func (suite *IPCDriverSuite) writeReadCompare(c *C, filename string, contents, expected []byte) bool {
	err := suite.DriverClient.PutContent(filename, contents)
	if err != nil {
		c.Error(err)
		return false
	}

	readContents, err := suite.DriverClient.GetContent(filename)
	if err != nil {
		c.Error(err)
		return false
	}

	if !bytes.Equal(contents, readContents) {
		c.Errorf("Expected: %s, got %s", contents, readContents)
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
