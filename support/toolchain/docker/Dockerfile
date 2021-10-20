FROM golang:1.17
ENV PATH /go/bin:/src/bin:/root/go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go:/src
RUN go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.42.1 && \
    go install golang.org/x/tools/cmd/...@latest && \
    go install github.com/go-delve/delve/cmd/dlv@latest && \
    curl -sfL https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s v2.8.1 && \
    chmod -R a+rw /go
