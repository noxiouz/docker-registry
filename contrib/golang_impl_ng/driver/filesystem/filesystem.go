package filesystem

import (
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type FilesystemDriver struct {
	rootDirectory string
}

func NewDriver(rootDirectory string) *FilesystemDriver {
	return &FilesystemDriver{rootDirectory}
}

func (d *FilesystemDriver) subPath(subPath string) string {
	return path.Join(d.rootDirectory, subPath)
}

func (d *FilesystemDriver) GetContent(path string) ([]byte, error) {
	contents, err := ioutil.ReadFile(d.subPath(path))
	if err != nil {
		return nil, driver.PathNotFoundError{path}
	}
	return contents, nil
}

func (d *FilesystemDriver) PutContent(subPath string, contents []byte) error {
	fullPath := d.subPath(subPath)
	parentDir := path.Dir(fullPath)
	err := os.MkdirAll(parentDir, 0755)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fullPath, contents, 0644)
	return err
}

func (d *FilesystemDriver) ReadStream(path string) (io.ReadCloser, error) {
	file, err := os.OpenFile(d.subPath(path), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (d *FilesystemDriver) WriteStream(subPath string, offset uint64, reader io.ReadCloser) error {
	defer reader.Close()

	resumableOffset, err := d.ResumeWritePosition(subPath)
	if err != nil {
		return err
	}
	if offset > resumableOffset {
		return driver.InvalidOffsetError{subPath, offset}
	}

	fullPath := d.subPath(subPath)
	parentDir := path.Dir(fullPath)
	err = os.MkdirAll(parentDir, 0755)
	if err != nil {
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 32*1024)
	for {
		bytesRead, er := reader.Read(buf)
		if bytesRead > 0 {
			bytesWritten, ew := file.WriteAt(buf[0:bytesRead], int64(offset))
			if bytesWritten > 0 {
				offset += uint64(bytesWritten)
			}
			if ew != nil {
				err = ew
				break
			}
			if bytesRead != bytesWritten {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return err
}

func (d *FilesystemDriver) ResumeWritePosition(subPath string) (uint64, error) {
	fullPath := d.subPath(subPath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return 0, err
	}
	return uint64(fileInfo.Size()), nil
}

func (d *FilesystemDriver) Move(sourcePath string, destPath string) error {
	err := os.Rename(d.subPath(sourcePath), d.subPath(destPath))
	return err
}

func (d *FilesystemDriver) Delete(path string) error {
	err := os.RemoveAll(d.subPath(path))
	return err
}
