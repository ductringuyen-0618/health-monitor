FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/monitor ./cmd/monitor

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/monitor /monitor
EXPOSE 8080
ENTRYPOINT ["/monitor"]
