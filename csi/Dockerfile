# Multi-stage build for ZeroFS CSI Driver
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

# Build the driver
RUN CGO_ENABLED=0 GOOS=linux go build \
    -a -ldflags '-extldflags "-static"' \
    -o /zerofsplugin \
    ./cmd/zerofsplugin

# Final stage - minimal runtime image
FROM alpine:3.19

# Install runtime dependencies
# - util-linux: for mount command
# - ca-certificates: for HTTPS
RUN apk add --no-cache \
    util-linux \
    ca-certificates \
    e2fsprogs \
    xfsprogs \
    blkid \
    file

# Note: 9P support requires the 9p kernel module (v9fs)
# This must be available in the host kernel, not the container
# Most modern Linux kernels include this by default

# Copy the binary from builder
COPY --from=builder /zerofsplugin /zerofsplugin

# Set the entrypoint
ENTRYPOINT ["/zerofsplugin"]
