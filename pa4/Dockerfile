FROM golang:1.23-alpine

RUN apk add --no-cache netcat-openbsd

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go stopword.txt seed-urls.txt ./
RUN go build -o /usr/local/bin/pa4 .

CMD ["pa4"]
