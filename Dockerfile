FROM convox/alpine:3.1

RUN apk-install git go

ENV GOPATH /go
ENV GOBIN $GOPATH/bin
ENV PATH $GOBIN:$PATH

WORKDIR /go/src/github.com/convox/ktail
COPY . /go/src/github.com/convox/ktail
RUN go get .

ENTRYPOINT ["/go/bin/ktail"]
