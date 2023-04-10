FROM golang:1.20 as builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build -o quackhouse quackhouse.go
RUN strip quackhouse
  
FROM ubuntu:20.04
COPY --from=builder /quackhouse /quackhouse
RUN echo "INSTALL httpfs;" | /quackhouse --stdin
RUN echo "INSTALL json;" | /quackhouse --stdin
RUN echo "INSTALL parquet;" | /quackhouse --stdin
RUN echo "INSTALL fts;" | /quackhouse --stdin
CMD ["/quackhouse"]
