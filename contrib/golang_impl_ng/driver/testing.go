package driver

import (
	"bytes"
	"io/ioutil"
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

func (suite *InProcessDriverSuite) TestWriteRead4(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestReadNonexistent(c *C) {
	filename := randomPath(32)
	_, err := suite.Driver.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams1(c *C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams2(c *C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams3(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams4(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestContinueStreamAppend(c *C) {
	filename := randomPath(32)

	chunkSize := uint64(32)

	contentsChunk1 := []byte(randomPath(chunkSize))
	contentsChunk2 := []byte(randomPath(chunkSize))
	contentsChunk3 := []byte(randomPath(chunkSize))

	err := suite.Driver.WriteStream(filename, 0, ioutil.NopCloser(bytes.NewReader(contentsChunk1)))
	c.Assert(err, IsNil)

	received, err := suite.Driver.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, contentsChunk1)

	offset, err := suite.Driver.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, chunkSize)

	err = suite.Driver.WriteStream(filename, offset, ioutil.NopCloser(bytes.NewReader(contentsChunk2)))
	c.Assert(err, IsNil)

	received, err = suite.Driver.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, append(contentsChunk1, contentsChunk2...))

	offset, err = suite.Driver.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, 2*chunkSize)

	err = suite.Driver.WriteStream(filename, offset, ioutil.NopCloser(bytes.NewReader(contentsChunk3)))
	c.Assert(err, IsNil)

	received, err = suite.Driver.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))

	offset, err = suite.Driver.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, uint64(3*chunkSize))
}

func (suite *InProcessDriverSuite) TestReadNonexistentStream(c *C) {
	filename := randomPath(32)
	_, err := suite.Driver.ReadStream(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemoveExisting(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.Driver.PutContent(filename, contents)
	c.Assert(err, IsNil)

	err = suite.Driver.Delete(filename)
	c.Assert(err, IsNil)

	_, err = suite.Driver.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemoveNonexistent(c *C) {
	filename := randomPath(32)
	err := suite.Driver.Delete(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemoveFolder(c *C) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.Driver.PutContent(path.Join(dirname, filename1), contents)
	c.Assert(err, IsNil)

	err = suite.Driver.PutContent(path.Join(dirname, filename2), contents)
	c.Assert(err, IsNil)

	err = suite.Driver.Delete(dirname)
	c.Assert(err, IsNil)

	_, err = suite.Driver.GetContent(path.Join(dirname, filename1))
	c.Assert(err, NotNil)

	_, err = suite.Driver.GetContent(path.Join(dirname, filename2))
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) writeReadCompare(c *C, filename string, contents, expected []byte) {
	err := suite.Driver.PutContent(filename, contents)
	c.Assert(err, IsNil)

	readContents, err := suite.Driver.GetContent(filename)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, contents)
}

func (suite *InProcessDriverSuite) writeReadCompareStreams(c *C, filename string, contents, expected []byte) {
	err := suite.Driver.WriteStream(filename, 0, ioutil.NopCloser(bytes.NewReader(contents)))
	c.Assert(err, IsNil)

	reader, err := suite.Driver.ReadStream(filename)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err := ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, contents)
}

var pathChars = []byte("abcdefghijklmnopqrstuvwxyz")

func randomPath(length uint64) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = pathChars[rand.Intn(len(pathChars))]
	}
	return string(b)
}
