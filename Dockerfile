FROM golang:1.23 AS builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build -o quackpipe .
RUN strip quackpipe
RUN apt update && apt install -y libgrpc-dev
  
FROM debian:12
COPY --from=builder /quackpipe /quackpipe
COPY --from=builder /usr/share/grpc/roots.pem /usr/share/grpc/roots.pem
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL motherduck; INSTALL fts; INSTALL chsql FROM community;  INSTALL chsql_native FROM community;" | /quackpipe --stdin
CMD ["/quackpipe"]
