package main

import (
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver/inmemory"
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver/ipc"
)

func main() {
	ipc.Server(inmemory.NewDriver())
}
