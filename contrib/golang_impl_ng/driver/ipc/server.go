package ipc

import (
	"app/driver"
	"fmt"
	"github.com/docker/libchan"
	"github.com/docker/libchan/spdy"
	"io"
	"net"
	"os"
)

func Server(driver driver.Driver) error {
	childSocket := os.NewFile(3, "childSocket")
	conn, err := net.FileConn(childSocket)
	if err != nil {
		panic(err)
	}
	if transport, err := spdy.NewServerTransport(conn); err != nil {
		panic(err)
	} else {
		for {
			receiver, err := transport.WaitReceiveChannel()
			if err != nil {
				panic(err)
			}

			go func (receiver libchan.Receiver) {
				var request Request
				err = receiver.Receive(&request)
				if err != nil {
					panic(err)
				}

				fmt.Fprintf(os.Stderr, "Received request: %#v\n", request)

				switch request.Type {
				case "GetContent":
					path, _ := request.Parameters["Path"].(string)
					content, err := driver.GetContent(path)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Contents": content, "Error": errorMessage})
					if err != nil {
						panic(err)
					}
				case "PutContent":
					path, _ := request.Parameters["Path"].(string)
					contents, _ := request.Parameters["Contents"].([]byte)
					err = driver.PutContent(path, contents)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Error": errorMessage})
					if err != nil {
						panic(err)
					}
				case "ReadStream":
					path, _ := request.Parameters["Path"].(string)
					reader, err := driver.ReadStream(path)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Reader": WrapReadCloser(reader), "Error": errorMessage})
					if err != nil {
						panic(err)
					}
				case "WriteStream":
					path, _ := request.Parameters["Path"].(string)
					reader, _ := request.Parameters["Reader"].(io.ReadCloser)
					err = driver.WriteStream(path, reader)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Error": errorMessage})
					if err != nil {
						panic(err)
					}
				case "Move":
					sourcePath, _ := request.Parameters["SourcePath"].(string)
					destPath, _ := request.Parameters["DestPath"].(string)
					err = driver.Move(sourcePath, destPath)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Error": errorMessage})
					if err != nil {
						panic(err)
					}
				case "Delete":
					path, _ := request.Parameters["Path"].(string)
					err = driver.Delete(path)
					var errorMessage string
					if err != nil {
						errorMessage = err.Error()
					}
					err = request.ResponseChannel.Send(map[string]interface{}{"Error": errorMessage})
					if err != nil {
						panic(err)
					}
				default:
					panic(request)
				}
			}(receiver)
		}
		return nil;
	}
}
