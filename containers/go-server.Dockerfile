# Build stage
FROM golang:1.25-bookworm AS builder
WORKDIR /app
COPY src/main/go/go.mod src/main/go/go.sum ./
RUN go mod download
COPY src/main/go/. ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ./cmd/server

# Runtime stage
FROM gcr.io/distroless/static-debian12
WORKDIR /
COPY --from=builder /server /server
USER nonroot:nonroot
ENTRYPOINT ["/server"]
