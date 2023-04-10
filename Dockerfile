FROM golang:1.20 as builder
WORKDIR /
COPY . .
RUN CGO_ENABLED=1 go build -o quackhouse quackhouse.go
RUN strip quackhouse
  
FROM ubuntu:20.04
COPY --from=builder /quackhouse /quackhouse
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL fts; INSTALL postgres_scanner;" | /quackhouse --stdin
CMD ["/quackhouse"]
