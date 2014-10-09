FROM golang:1.3.3

WORKDIR /go/src/

RUN ["mkdir", "-p", "github.com/docker/"]

VOLUME ["/Users/bbland/code/docker/docker-registry:github.com/docker/docker-registry"]

# WORKDIR github.com/docker/docker-registry/contrib/golang_impl_ng

RUN ["go", "get", "-d", "-v", "./..."]
RUN ["go", "fmt", "./..."]
RUN ["go", "install", "./..."]
ENTRYPOINT ["/go/bin/driverclient", "-driver"]
CMD ["inmemory"]
