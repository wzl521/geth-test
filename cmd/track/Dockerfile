FROM golang:1.21-alpine as builder

RUN apk add --no-cache gcc musl-dev linux-headers git

ADD . /go-ethereum
RUN cd /go-ethereum && go build -o ./cmd/track/geth-track ./cmd/track

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /go-ethereum/cmd/track/geth-track /usr/local/bin

ENTRYPOINT ["geth-track"]
