FROM golang:1.20.5-alpine3.18

RUN mkdir /app

RUN apk --no-cache update

# copy dependency reqs first, for cache efficiency
COPY go.mod go.sum /app/

# use application dir as working directory
WORKDIR /app

# download go dependencies
RUN go mod download

COPY . /app

# run our tests
RUN go test -v

# build our app
RUN go build -o main .

# when container boots, run our application
CMD ["/app/main"]