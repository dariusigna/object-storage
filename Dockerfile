FROM golang:1.23-alpine
WORKDIR /mnt/homework
COPY . .
RUN go build -o object-storage ./cmd/gateway.go

# Docker is used as a base image so you can easily start playing around in the container using the Docker command line client.
FROM docker
COPY --from=0 /mnt/homework/object-storage /usr/local/bin/object-storage

# Set the entrypoint to run the server
ENTRYPOINT ["/usr/local/bin/object-storage"]
