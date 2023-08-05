FROM golang:1.20 as builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build -o quackpipe quackpipe.go
RUN strip quackpipe
  
FROM debian:12
COPY --from=builder /quackpipe /quackpipe
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL fts;" | /quackpipe --stdin
CMD ["/quackpipe"]
