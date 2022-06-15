FROM golang:1.17 AS builder

WORKDIR /go/src/github.com/karmada-io/karmada
COPY . /go/src/github.com/karmada-io/karmada

RUN make karmada-controller-manager
RUN mv /go/src/github.com/karmada-io/karmada/karmada-controller-manager /bin/


FROM alpine:3.15.1

RUN apk add --no-cache ca-certificates

COPY main /bin/karmada-aggregated-apiserver

CMD ["/bin/karmada-aggregated-apiserver"]