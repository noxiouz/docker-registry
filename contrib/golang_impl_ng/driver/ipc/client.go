package ipc

import (
	"encoding/json"
	"errors"
	"github.com/docker/libchan"
	"github.com/docker/libchan/spdy"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"syscall"
)

type DriverClient struct {
	subprocess *exec.Cmd
	socket     *os.File
	transport  *spdy.Transport
}

func NewDriverClient(name string, parameters map[string]string) (*DriverClient, error) {
	paramsBytes, err := json.Marshal(parameters)
	if err != nil {
		return nil, err
	}

	driverPath := os.ExpandEnv(path.Join("$GOPATH", "bin", name))
	if _, err := os.Stat(driverPath); os.IsNotExist(err) {
		driverPath = path.Join(path.Dir(os.Args[0]), name)
	}
	if _, err := os.Stat(driverPath); os.IsNotExist(err) {
		driverPath, err = exec.LookPath(name)
		if err != nil {
			return nil, err
		}
	}

	command := exec.Command(driverPath, string(paramsBytes))

	return &DriverClient{
		subprocess: command,
	}, nil
}

func (d *DriverClient) Start() error {
	fileDescriptors, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}

	childSocket := os.NewFile(uintptr(fileDescriptors[0]), "childSocket")
	parentSocket := os.NewFile(uintptr(fileDescriptors[1]), "parentSocket")

	d.subprocess.Stdout = os.Stdout
	d.subprocess.Stderr = os.Stderr
	d.subprocess.ExtraFiles = []*os.File{childSocket}

	if err = d.subprocess.Start(); err != nil {
		parentSocket.Close()
		return err
	}

	if err = childSocket.Close(); err != nil {
		parentSocket.Close()
		return err
	}

	connection, err := net.FileConn(parentSocket)
	if err != nil {
		parentSocket.Close()
		return err
	}
	transport, err := spdy.NewClientTransport(connection)
	if err != nil {
		parentSocket.Close()
		return err
	}
	d.socket = parentSocket
	d.transport = transport

	return nil
}

func (d *DriverClient) Stop() error {
	closeTransportErr := d.transport.Close()
	closeSocketErr := d.socket.Close()
	killErr := d.subprocess.Process.Kill()

	if closeTransportErr != nil {
		return closeTransportErr
	} else if closeSocketErr != nil {
		return closeSocketErr
	}
	return killErr
}

func (d *DriverClient) GetContent(path string) ([]byte, error) {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return nil, err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err = sender.Send(&Request{Type: "GetContent", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return nil, err
	}

	responseBytes, _ := response["Contents"].([]byte)
	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return responseBytes, err
}

func (d *DriverClient) PutContent(path string, contents []byte) error {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path, "Contents": contents}
	err = sender.Send(&Request{Type: "PutContent", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return err
}

func (d *DriverClient) ReadStream(path string) (io.ReadCloser, error) {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return nil, err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err = sender.Send(&Request{Type: "ReadStream", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return nil, err
	}

	reader, _ := response["Reader"].(io.ReadCloser)
	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return reader, err
}

func (d *DriverClient) WriteStream(path string, offset uint64, reader io.ReadCloser) error {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path, "Offest": offset, "Reader": WrapReadCloser(reader)}
	err = sender.Send(&Request{Type: "WriteStream", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return err
}

func (d *DriverClient) ResumeWritePosition(path string) (uint64, error) {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return 0, err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err = sender.Send(&Request{Type: "ResumeWritePosition", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return 0, err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return 0, err
	}

	offset, _ := response["Offset"].(uint64)
	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return offset, err
}

func (d *DriverClient) Move(sourcePath string, destPath string) error {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"SourcePath": sourcePath, "DestPath": destPath}
	err = sender.Send(&Request{Type: "Move", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return err
}

func (d *DriverClient) Delete(path string) error {
	sender, err := d.transport.NewSendChannel()
	if err != nil {
		return err
	}

	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err = sender.Send(&Request{Type: "Delete", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response map[string]interface{}
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	errorMessage, _ := response["Error"].(string)
	if errorMessage != "" {
		err = errors.New(errorMessage)
	}
	return err
}
