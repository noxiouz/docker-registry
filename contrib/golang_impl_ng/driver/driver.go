package driver

import (
	"fmt"
	"io"
)

type Driver interface {
	GetContent(path string) ([]byte, error)
	PutContent(path string, content []byte) error
	ReadStream(path string) (io.ReadCloser, error)
	WriteStream(path string, readCloser io.ReadCloser) error
	Move(sourcePath string, destPath string) error
	Delete(path string) error
}

func ImageManifestPath(imageId string) string {
	return fmt.Sprintf("images/%s/manifest.json", imageId)
}

func ImagePrivatePath(imageId string) string {
	return fmt.Sprintf("images/%s/_private", imageId)
}

func ImageDeletionPath(imageId string) string {
	return fmt.Sprintf("images/%s/_deleted", imageId)
}

func ImageLayerPath(layerTarsum string) string {
	return fmt.Sprintf("layers/%s/layer", layerTarsum)
}

func LayerChecksumPath(layerTarsum string) string {
	return fmt.Sprintf("layers/%s/checksum", layerTarsum)
}

type PathNotFoundError struct {
	Path string
}

func (err PathNotFoundError) Error() string {
	return fmt.Sprintf("Path not found: %s", err.Path)
}
