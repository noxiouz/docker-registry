package main

import (
	"app/driver/ipc"
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
)

var driverName = flag.String("driver", "inmemory", "specify the name of the storage driver to use")

func main() {
	client, err := ipc.NewDriverClient(*driverName, nil)
	if err != nil {
		panic(err)
	}
	err = client.Start()
	if err != nil {
		panic(err)
	}

	fmt.Println("Putting hello world")
	err = client.PutContent("hello", []byte("world"))
	if err != nil {
		panic(err)
	}
	fmt.Println("Getting 'hello'")
	contents, err := client.GetContent("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(contents))
	
	fmt.Println("Putting a stream")
	err = client.WriteStream("stream", bytes.NewReader([]byte("this is a stream")))
	if err != nil {
		panic(err)
	}
	fmt.Println("Getting 'stream'")
	reader, err := client.ReadStream("stream")
	if err != nil {
		panic(err)
	}
	contents, err = ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(contents))

	fmt.Println("Moving 'hello' -> 'goodbye'")
	err = client.Move("hello", "goodbye")
	if err != nil {
		panic(err)
	}

	fmt.Println("Getting 'goodbye'")
	contents, err = client.GetContent("goodbye")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(contents))

	fmt.Println("Deleting 'goodbye'")
	err = client.Delete("goodbye")
	if err != nil {
		panic(err)
	}
	fmt.Println("Getting 'goodbye' again, (should fail)")
	contents, err = client.GetContent("goodbye")
	if err == nil {
		panic("Expected Delete(\"goodbye\") to fail")
	}
	fmt.Printf("Received error message: %s\n", err.Error())

	fmt.Println("Bracing for failure!!!")
	err = client.WriteStream("failboat", rand.Reader)
}
