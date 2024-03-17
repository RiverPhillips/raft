FROM golang:1.22 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./ ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /raft

# Deploy the application binary into a lean image
FROM gcr.io/distroless/base-debian11

WORKDIR /

COPY --from=build-stage /raft /raft

USER nonroot:nonroot

ENTRYPOINT ["/raft"]