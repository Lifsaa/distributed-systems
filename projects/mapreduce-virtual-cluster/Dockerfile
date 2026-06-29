FROM golang:1.23-alpine

WORKDIR /app

COPY go.mod .
COPY *.go .

RUN go build -o mapreduce .

CMD ["./mapreduce"]