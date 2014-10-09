package main

import (
	"app/driver"
	"app/driver/ipc"
	"encoding/json"
	"os"
)

func main() {
	parametersBytes := []byte(os.Args[1])
	var parameters map[string]interface{}
	err := json.Unmarshal(parametersBytes, &parameters)
	if err != nil {
		panic(err)
	}
	rootDirectory := "/tmp/registry"
	if parameters != nil {
		rootDirParam, ok := parameters["RootDirectory"].(string)
		if ok && rootDirParam != "" {
			rootDirectory = rootDirParam
		}
	} 
	ipc.Server(driver.NewFilesystemDriver(rootDirectory))
}
