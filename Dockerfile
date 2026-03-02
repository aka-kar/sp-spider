# SP-Spider Dockerfile
FROM golang:1.21-bookworm AS builder

# Install sqlite development libraries and build tools for CGO
RUN apt-get update && apt-get install -y sqlite3 gcc

# Set working directory
WORKDIR /app

# Set Go proxy and module cache
ENV GOPROXY=direct
ENV GOCACHE=/root/.cache/go-build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build spider with CGO enabled for sqlite
ENV CGO_ENABLED=1
RUN go build -o sp-spider .

# Final stage
FROM debian:bookworm-slim

# Install sqlite runtime libraries
RUN apt-get update && apt-get install -y sqlite3

# Create app directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/sp-spider .

# Create data directory for persistence
RUN mkdir -p /app/data

# Make binary executable
RUN chmod +x ./sp-spider

# Expose web server port
EXPOSE 8080

# Default command - start spider server
CMD ["./sp-spider"]

# Labels
LABEL maintainer="SP-Spider"
LABEL version="1.0"
