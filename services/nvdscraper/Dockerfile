FROM golang:1.20.5-alpine3.18

RUN mkdir /app

RUN apk --no-cache update && apk --no-cache add git gcc libc-dev musl-dev

# https://stackoverflow.com/a/69030479
ENV CGO_ENABLED 1

ADD . /app

# use application dir as working directory
WORKDIR /app

# TODO do I need this?
RUN go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka

# download go dependencies
RUN go mod download

# build our app - musl used due to https://github.com/confluentinc/confluent-kafka-go/issues/454
RUN go build -o main -tags musl .

# when container boots, run our application
CMD ["/app/main"]