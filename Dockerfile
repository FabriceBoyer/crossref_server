FROM golang:1.21-bullseye

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . ./
RUN go build -v -o /crossref_server
CMD [ "/crossref_server" ]

# CMD ["go", "run", "main.go"]
