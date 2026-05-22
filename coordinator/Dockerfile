FROM --platform=linux/arm64 golang:1.22-alpine AS builder

ARG TARGETARCH=arm64

RUN apk add --no-cache git ca-certificates wget

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /bin/coordinator ./cmd/coordinator

FROM --platform=linux/arm64 alpine:3.19

RUN apk add --no-cache wget ca-certificates

COPY --from=builder /bin/coordinator /coordinator

EXPOSE 8080 9091

ENTRYPOINT ["/coordinator"]