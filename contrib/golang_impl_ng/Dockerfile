FROM golang:1.3.3

RUN ["mkdir", "-p", "/go/src/app"]
WORKDIR /go/src/app
COPY . /go/src/app

RUN ["go", "get", "-d", "-v", "./..."]
RUN ["go", "install", "./..."]
CMD ["/go/bin/driverclient"]
