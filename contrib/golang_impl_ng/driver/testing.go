package driver

import (
	"bytes"
	"math/rand"
	"path"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner
func Test(t *testing.T) { TestingT(t) }

type InProcessDriverSuite struct {
	DriverConstructor func() (Driver, error)
	Driver
}

type TestDriverConfig struct {
	name   string
	params map[string]string
}

func (suite *InProcessDriverSuite) SetUpSuite(c *C) {
	d, err := suite.DriverConstructor()
	if err != nil {
		c.Fatal(err)
	}
	suite.Driver = d
}

func (suite *InProcessDriverSuite) TestWriteRead1(c *C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteRead2(c *C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteRead3(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestRemoveExisting(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.Driver.PutContent(filename, contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.Driver.Delete(filename)
	if err != nil {
		c.Error(err)
		return
	}

	_, err = suite.Driver.GetContent(filename)
	if err == nil {
		c.Errorf("%s should not exist", filename)
		return
	}
}

func (suite *InProcessDriverSuite) TestRemoveFolder(c *C) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.Driver.PutContent(path.Join(dirname, filename1), contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.Driver.PutContent(path.Join(dirname, filename2), contents)
	if err != nil {
		c.Error(err)
		return
	}

	err = suite.Driver.Delete(dirname)
	if err != nil {
		c.Error(err)
		return
	}

	_, err = suite.Driver.GetContent(path.Join(dirname, filename1))
	if err == nil {
		c.Errorf("%s should not exist", path.Join(dirname, filename1))
		return
	}

	_, err = suite.Driver.GetContent(path.Join(dirname, filename2))
	if err == nil {
		c.Errorf("%s should not exist", path.Join(dirname, filename2))
		return
	}
}

func (suite *InProcessDriverSuite) writeReadCompare(c *C, filename string, contents, expected []byte) bool {
	err := suite.Driver.PutContent(filename, contents)
	if err != nil {
		c.Error(err)
		return false
	}

	readContents, err := suite.Driver.GetContent(filename)
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
