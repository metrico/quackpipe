FROM golang:1.20 as builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build \
  -ldflags "-linkmode external -extldflags -static" \
  -o quackhouse \
  -a quackhouse.go
RUN strip quackhouse
  
FROM scratch
COPY --from=builder /quackhouse /quackhouse
CMD ["/quackhouse"]
