package driver

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

type FilesystemDriver struct {
	rootDirectory string
}

func NewFilesystemDriver(rootDirectory string) *FilesystemDriver {
	return &FilesystemDriver{rootDirectory}
}

func (d *FilesystemDriver) subPath(subPath string) string {
	return path.Join(d.rootDirectory, subPath)
}

func (d *FilesystemDriver) GetContent(path string) ([]byte, error) {
	contents, err := ioutil.ReadFile(d.subPath(path))
	if err != nil {
		return nil, PathNotFoundError{path}
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

func (d *FilesystemDriver) WriteStream(subPath string, reader io.ReadCloser) error {
	defer reader.Close()
	
	fullPath := d.subPath(subPath)
	parentDir := path.Dir(fullPath)
	err := os.MkdirAll(parentDir, 0755)
	if err != nil {
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	return err
}

func (d *FilesystemDriver) Move(sourcePath string, destPath string) error {
	err := os.Rename(d.subPath(sourcePath), d.subPath(destPath))
	return err
}

func (d *FilesystemDriver) Delete(path string) error {
	err := os.Remove(d.subPath(path))
	return err
}
