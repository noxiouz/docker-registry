package driver

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"path"
	"sort"
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
	filename := randomString(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteRead2(c *C) {
	filename := randomString(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteRead3(c *C) {
	filename := randomString(32)
	contents := []byte(randomString(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteRead4(c *C) {
	filename := randomString(32)
	contents := []byte(randomString(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestReadNonexistent(c *C) {
	filename := randomString(32)
	_, err := suite.Driver.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams1(c *C) {
	filename := randomString(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams2(c *C) {
	filename := randomString(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams3(c *C) {
	filename := randomString(32)
	contents := []byte(randomString(32))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestWriteReadStreams4(c *C) {
	filename := randomString(32)
	contents := []byte(randomString(1024 * 1024))
	suite.writeReadCompare(c, filename, contents, contents)
}

func (suite *InProcessDriverSuite) TestContinueStreamAppend(c *C) {
	filename := randomString(32)

	chunkSize := uint64(32)

	contentsChunk1 := []byte(randomString(chunkSize))
	contentsChunk2 := []byte(randomString(chunkSize))
	contentsChunk3 := []byte(randomString(chunkSize))

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

func (suite *InProcessDriverSuite) TestReadStreamWithOffset(c *C) {
	filename := randomString(32)

	chunkSize := uint64(32)

	contentsChunk1 := []byte(randomString(chunkSize))
	contentsChunk2 := []byte(randomString(chunkSize))
	contentsChunk3 := []byte(randomString(chunkSize))

	err := suite.Driver.PutContent(filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	c.Assert(err, IsNil)

	reader, err := suite.Driver.ReadStream(filename, 0)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err := ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))

	reader, err = suite.Driver.ReadStream(filename, chunkSize)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, append(contentsChunk2, contentsChunk3...))

	reader, err = suite.Driver.ReadStream(filename, chunkSize*2)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, contentsChunk3)

	reader, err = suite.Driver.ReadStream(filename, chunkSize*3)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err = ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, []byte{})
}

func (suite *InProcessDriverSuite) TestReadNonexistentStream(c *C) {
	filename := randomString(32)
	_, err := suite.Driver.ReadStream(filename, 0)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestList(c *C) {
	rootDirectory := randomString(uint64(8 + rand.Intn(8)))
	parentDirectory := rootDirectory + "/" + randomString(uint64(8+rand.Intn(8)))
	childFiles := make([]string, 50)
	for i := 0; i < len(childFiles); i++ {
		childFile := parentDirectory + "/" + randomString(uint64(8+rand.Intn(8)))
		childFiles[i] = childFile
		err := suite.Driver.PutContent(childFile, []byte(randomString(32)))
		c.Assert(err, IsNil)
	}
	sort.Strings(childFiles)

	keys, err := suite.Driver.List(rootDirectory)
	c.Assert(err, IsNil)
	c.Assert(keys, DeepEquals, []string{parentDirectory})

	keys, err = suite.Driver.List(parentDirectory)
	c.Assert(err, IsNil)

	sort.Strings(keys)
	c.Assert(keys, DeepEquals, childFiles)
}

func (suite *InProcessDriverSuite) TestMove(c *C) {
	contents := []byte(randomString(32))
	sourcePath := randomString(32)
	destPath := randomString(32)

	err := suite.Driver.PutContent(sourcePath, contents)
	c.Assert(err, IsNil)

	err = suite.Driver.Move(sourcePath, destPath)
	c.Assert(err, IsNil)

	received, err := suite.Driver.GetContent(destPath)
	c.Assert(err, IsNil)
	c.Assert(received, DeepEquals, contents)

	_, err = suite.Driver.GetContent(sourcePath)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestMoveNonexistent(c *C) {
	sourcePath := randomString(32)
	destPath := randomString(32)

	err := suite.Driver.Move(sourcePath, destPath)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemove(c *C) {
	filename := randomString(32)
	contents := []byte(randomString(32))

	err := suite.Driver.PutContent(filename, contents)
	c.Assert(err, IsNil)

	err = suite.Driver.Delete(filename)
	c.Assert(err, IsNil)

	_, err = suite.Driver.GetContent(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemoveNonexistent(c *C) {
	filename := randomString(32)
	err := suite.Driver.Delete(filename)
	c.Assert(err, NotNil)
}

func (suite *InProcessDriverSuite) TestRemoveFolder(c *C) {
	dirname := randomString(32)
	filename1 := randomString(32)
	filename2 := randomString(32)
	contents := []byte(randomString(32))

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

	reader, err := suite.Driver.ReadStream(filename, 0)
	c.Assert(err, IsNil)
	defer reader.Close()

	readContents, err := ioutil.ReadAll(reader)
	c.Assert(err, IsNil)

	c.Assert(readContents, DeepEquals, contents)
}

var pathChars = []byte("abcdefghijklmnopqrstuvwxyz")

func randomString(length uint64) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = pathChars[rand.Intn(len(pathChars))]
	}
	return string(b)
}
