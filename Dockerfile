FROM golang:alpine AS builder
WORKDIR /go/src
RUN apk add git build-base && git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go
COPY ./ .
RUN mage -v build

FROM alpine

# Add pv as a user
RUN apk add tzdata && adduser -D pv
# Run pv as non-privileged
USER pv
WORKDIR /home/pv

COPY --from=builder /go/src/pvdb-metrics /home/pv
CMD "/home/pv/pvdb-metrics"
