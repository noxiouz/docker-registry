package ipc

import (
	"fmt"
	"github.com/docker/docker-registry/contrib/golang_impl_ng/driver"
	"github.com/docker/libchan"
	"github.com/docker/libchan/spdy"
	"io"
	"net"
	"os"
)

func Server(driver driver.Driver) error {
	childSocket := os.NewFile(3, "childSocket")
	defer childSocket.Close()
	conn, err := net.FileConn(childSocket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	if transport, err := spdy.NewServerTransport(conn); err != nil {
		panic(err)
	} else {
		for {
			receiver, err := transport.WaitReceiveChannel()
			if err != nil {
				panic(err)
			}
			go receive(driver, receiver)
		}
		return nil
	}
}

func receive(driver driver.Driver, receiver libchan.Receiver) {
	for {
		var request Request
		err := receiver.Receive(&request)
		if err != nil {
			panic(err)
		}
		go handleRequest(driver, request)
	}
}

func handleRequest(driver driver.Driver, request Request) {
	fmt.Fprintf(os.Stderr, "Received request: %#v\n", request)

	switch request.Type {
	case "GetContent":
		path, _ := request.Parameters["Path"].(string)
		content, err := driver.GetContent(path)
		response := GetContentResponse{
			Content: content,
			Error:   ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "PutContent":
		path, _ := request.Parameters["Path"].(string)
		contents, _ := request.Parameters["Contents"].([]byte)
		err := driver.PutContent(path, contents)
		response := PutContentResponse{
			Error: ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "ReadStream":
		path, _ := request.Parameters["Path"].(string)
		reader, err := driver.ReadStream(path)
		response := ReadStreamResponse{
			Reader: WrapReadCloser(reader),
			Error:  ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "WriteStream":
		path, _ := request.Parameters["Path"].(string)
		offset, _ := request.Parameters["Offset"].(uint64)
		reader, _ := request.Parameters["Reader"].(io.ReadCloser)
		err := driver.WriteStream(path, offset, reader)
		response := WriteStreamResponse{
			Error: ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "ResumeWritePosition":
		path, _ := request.Parameters["Path"].(string)
		position, err := driver.ResumeWritePosition(path)
		response := ResumeWritePositionResponse{
			Position: position,
			Error:    ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "Move":
		sourcePath, _ := request.Parameters["SourcePath"].(string)
		destPath, _ := request.Parameters["DestPath"].(string)
		err := driver.Move(sourcePath, destPath)
		response := MoveResponse{
			Error: ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	case "Delete":
		path, _ := request.Parameters["Path"].(string)
		err := driver.Delete(path)
		response := DeleteResponse{
			Error: ResponseError(err),
		}
		err = request.ResponseChannel.Send(&response)
		if err != nil {
			panic(err)
		}
	default:
		panic(request)
	}
}
