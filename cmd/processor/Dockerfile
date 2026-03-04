FROM golang:1.24 AS builder

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
# CGO_ENABLED=0 works because modernc.org/sqlite is a pure-Go SQLite port.
RUN CGO_ENABLED=0 go build -o processor ./cmd/processor

# Copy binary into slim image
FROM alpine
RUN mkdir -p /data
WORKDIR /app
COPY --from=builder /src/processor .
CMD ["/app/processor"]
