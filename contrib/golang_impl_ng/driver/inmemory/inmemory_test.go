package inmemory

import (
	"testing"

	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver/ipc"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var filesystemDriverConstructor = func() (driver.Driver, error) {
	return NewDriver(), nil
}

var InProcessSuite = Suite(&driver.InProcessDriverSuite{
	DriverConstructor: filesystemDriverConstructor,
})

var IPCSuite = Suite(&ipc.IPCDriverSuite{
	TestDriverConfig: &ipc.TestDriverConfig{
		Name: "inmemory",
	},
})
