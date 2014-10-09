package main

import (
	"app/driver"
	"app/driver/ipc"
)

func main() {
	ipc.Server(driver.NewInMemoryDriver())
}
