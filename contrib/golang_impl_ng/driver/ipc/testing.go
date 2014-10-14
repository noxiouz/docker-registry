package ipc

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
	c.Assert(err, IsNil)
	err = d.Start()
	c.Assert(err, IsNil)
	suite.DriverClient = d
}

func (suite *IPCDriverSuite) TearDownSuite(c *C) {
	err := suite.DriverClient.Stop()
	c.Assert(err, IsNil)
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

func (suite *IPCDriverSuite) TestWriteRead4(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestReadNonexistent(c *C) {
	filename := randomPath(32)
	_, err := suite.DriverClient.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *IPCDriverSuite) TestWriteReadStreams1(c *C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestWriteReadStreams2(c *C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestWriteReadStreams3(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestWriteReadStreams4(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *IPCDriverSuite) TestContinueStreamAppend(c *C) {
	filename := randomPath(32)

	chunkSize := uint64(32)

	contentsChunk1 := []byte(randomPath(chunkSize))
	contentsChunk2 := []byte(randomPath(chunkSize))
	contentsChunk3 := []byte(randomPath(chunkSize))

	err := suite.DriverClient.WriteStream(filename, 0, ioutil.NopCloser(bytes.NewReader(contentsChunk1)))
	c.Assert(err, IsNil)

	received, err := suite.DriverClient.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, contentsChunk1)

	offset, err := suite.DriverClient.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, chunkSize)

	err = suite.DriverClient.WriteStream(filename, offset, ioutil.NopCloser(bytes.NewReader(contentsChunk2)))
	c.Assert(err, IsNil)

	received, err = suite.DriverClient.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, append(contentsChunk1, contentsChunk2...))

	offset, err = suite.DriverClient.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, 2*chunkSize)

	err = suite.DriverClient.WriteStream(filename, offset, ioutil.NopCloser(bytes.NewReader(contentsChunk3)))
	c.Assert(err, IsNil)

	received, err = suite.DriverClient.GetContent(filename)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))

	offset, err = suite.DriverClient.ResumeWritePosition(filename)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, uint64(3*chunkSize))
}

func (suite *IPCDriverSuite) TestReadStreamWithOffset(c *C) {
	filename := randomPath(32)

	chunkSize := uint64(32)

	contentsChunk1 := []byte(randomPath(chunkSize))
	contentsChunk2 := []byte(randomPath(chunkSize))
	contentsChunk3 := []byte(randomPath(chunkSize))

	err := suite.DriverClient.PutContent(filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	c.Assert(err, IsNil)

	reader, err := suite.DriverClient.ReadStream(filename, 0)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err := ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))

	reader, err = suite.DriverClient.ReadStream(filename, chunkSize)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, append(contentsChunk2, contentsChunk3...))

	reader, err = suite.DriverClient.ReadStream(filename, chunkSize*2)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, contentsChunk3)

	reader, err = suite.DriverClient.ReadStream(filename, chunkSize*3)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, []byte{})
}

func (suite *IPCDriverSuite) TestReadNonexistentStream(c *C) {
	filename := randomPath(32)
	_, err := suite.DriverClient.ReadStream(filename, 0)
	c.Assert(err, NotNil)
}

func (suite *IPCDriverSuite) TestRemoveExisting(c *C) {
	filename := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.DriverClient.PutContent(filename, contents)
	c.Assert(err, IsNil)

	err = suite.DriverClient.Delete(filename)
	c.Assert(err, IsNil)

	_, err = suite.DriverClient.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *IPCDriverSuite) TestRemoveNonexistent(c *C) {
	filename := randomPath(32)
	err := suite.DriverClient.Delete(filename)
	c.Assert(err, NotNil)
}

func (suite *IPCDriverSuite) TestRemoveFolder(c *C) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	contents := []byte(randomPath(32))

	err := suite.DriverClient.PutContent(path.Join(dirname, filename1), contents)
	c.Assert(err, IsNil)

	err = suite.DriverClient.PutContent(path.Join(dirname, filename2), contents)
	c.Assert(err, IsNil)

	err = suite.DriverClient.Delete(dirname)
	c.Assert(err, IsNil)

	_, err = suite.DriverClient.GetContent(path.Join(dirname, filename1))
	c.Assert(err, NotNil)

	_, err = suite.DriverClient.GetContent(path.Join(dirname, filename2))
	c.Assert(err, NotNil)
}

func (suite *IPCDriverSuite) writeReadCompare(c *C, filename string, contents, expected []byte) {
	err := suite.DriverClient.PutContent(filename, contents)
	c.Assert(err, IsNil)

	readContents, err := suite.DriverClient.GetContent(filename)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, readContents)
}

func (suite *IPCDriverSuite) writeReadCompareStreams(c *C, filename string, contents, expected []byte) {
	err := suite.DriverClient.WriteStream(filename, 0, ioutil.NopCloser(bytes.NewReader(contents)))
	c.Assert(err, IsNil)

	reader, err := suite.DriverClient.ReadStream(filename, 0)
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
