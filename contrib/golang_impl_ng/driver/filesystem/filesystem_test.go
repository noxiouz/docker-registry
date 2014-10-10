package filesystem

import (
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver/ipc"
	. "gopkg.in/check.v1"
	"os"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var rootDirectory = "/tmp/driver"
var _ = os.RemoveAll(rootDirectory)

var filesystemDriverConstructor = func() (driver.Driver, error) {
	return NewDriver(rootDirectory), nil
}

var InProcessSuite = Suite(&driver.InProcessDriverSuite{
	DriverConstructor: filesystemDriverConstructor,
})

var IPCSuite = Suite(&ipc.IPCDriverSuite{
	TestDriverConfig: &ipc.TestDriverConfig{
		"filesystem",
		map[string]string{"RootDirectory": rootDirectory},
	},
})
