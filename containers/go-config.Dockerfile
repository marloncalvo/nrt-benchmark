# Build stage
FROM golang:1.25-bookworm AS builder
WORKDIR /app
COPY src/main/go/go.mod src/main/go/go.sum ./
RUN go mod download
COPY src/main/go/. ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /config ./cmd/config

# Runtime stage
FROM gcr.io/distroless/static-debian12
WORKDIR /
COPY --from=builder /config /config
USER nonroot:nonroot
ENTRYPOINT ["/config"]
