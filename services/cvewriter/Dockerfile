FROM golang:1.20.5-alpine3.18

RUN mkdir /app

RUN apk --no-cache update

ADD . /app

# use application dir as working directory
WORKDIR /app

# download go dependencies
RUN go mod download

# build our app
RUN go build -o main .

# when container boots, run our application
CMD ["/app/main"]