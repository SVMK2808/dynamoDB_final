FROM golang:1.18-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build -o dynamo-node .

# Start a new stage from scratch
FROM alpine:3.18

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/dynamo-node .
COPY --from=builder /app/configs ./configs

# Expose ports (can be overridden)
EXPOSE 7000

# Command to run the executable
ENTRYPOINT ["./dynamo-node"]
