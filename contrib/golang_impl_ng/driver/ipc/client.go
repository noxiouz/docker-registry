package ipc

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"syscall"

	"github.com/docker/libchan"
	"github.com/docker/libchan/spdy"
)

type DriverClient struct {
	subprocess *exec.Cmd
	socket     *os.File
	transport  *spdy.Transport
	sender     libchan.Sender
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
	sender, err := transport.NewSendChannel()
	if err != nil {
		transport.Close()
		parentSocket.Close()
		return err
	}

	d.socket = parentSocket
	d.transport = transport
	d.sender = sender

	return nil
}

func (d *DriverClient) Stop() error {
	closeSenderErr := d.sender.Close()
	closeTransportErr := d.transport.Close()
	closeSocketErr := d.socket.Close()
	killErr := d.subprocess.Process.Kill()

	if closeSenderErr != nil {
		return closeSenderErr
	} else if closeTransportErr != nil {
		return closeTransportErr
	} else if closeSocketErr != nil {
		return closeSocketErr
	}
	return killErr
}

func (d *DriverClient) GetContent(path string) ([]byte, error) {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err := d.sender.Send(&Request{Type: "GetContent", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return nil, err
	}

	var response GetContentResponse
	err = receiver.Receive(&response)
	if err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Content, nil
}

func (d *DriverClient) PutContent(path string, contents []byte) error {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path, "Contents": contents}
	err := d.sender.Send(&Request{Type: "PutContent", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response PutContentResponse
	err = receiver.Receive(&response)
	if err != nil {
		panic(err)
		return err
	}

	if response.Error != nil {
		return response.Error
	}

	return nil
}

func (d *DriverClient) ReadStream(path string) (io.ReadCloser, error) {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err := d.sender.Send(&Request{Type: "ReadStream", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return nil, err
	}

	var response ReadStreamResponse
	err = receiver.Receive(&response)
	if err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Reader, nil
}

func (d *DriverClient) WriteStream(path string, offset uint64, reader io.ReadCloser) error {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path, "Offest": offset, "Reader": WrapReadCloser(reader)}
	err := d.sender.Send(&Request{Type: "WriteStream", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response WriteStreamResponse
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	if response.Error != nil {
		return response.Error
	}

	return nil
}

func (d *DriverClient) ResumeWritePosition(path string) (uint64, error) {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err := d.sender.Send(&Request{Type: "ResumeWritePosition", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return 0, err
	}

	var response ResumeWritePositionResponse
	err = receiver.Receive(&response)
	if err != nil {
		return 0, err
	}

	if response.Error != nil {
		return 0, response.Error
	}

	return response.Position, nil
}

func (d *DriverClient) Move(sourcePath string, destPath string) error {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"SourcePath": sourcePath, "DestPath": destPath}
	err := d.sender.Send(&Request{Type: "Move", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response MoveResponse
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	if response.Error != nil {
		return response.Error
	}

	return nil
}

func (d *DriverClient) Delete(path string) error {
	receiver, remoteSender := libchan.Pipe()

	params := map[string]interface{}{"Path": path}
	err := d.sender.Send(&Request{Type: "Delete", Parameters: params, ResponseChannel: remoteSender})
	if err != nil {
		return err
	}

	var response DeleteResponse
	err = receiver.Receive(&response)
	if err != nil {
		return err
	}

	if response.Error != nil {
		return response.Error
	}

	return nil
}
