FROM golang:1.20 AS builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build -o quackpipe quackpipe.go
RUN strip quackpipe
RUN apt update && apt install -y libgrpc-dev
  
FROM debian:12
COPY --from=builder /quackpipe /quackpipe
COPY --from=builder /usr/share/grpc/roots.pem /usr/share/grpc/roots.pem
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL fts;" | /quackpipe --stdin
CMD ["/quackpipe"]
