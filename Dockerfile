# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY scripts/collect-licenses.sh ./scripts/collect-licenses.sh

ARG TARGETOS
ARG TARGETARCH
RUN --network=none --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/kafka-connect-healer ./cmd/kafka-connect-healer && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH sh scripts/collect-licenses.sh /out/licenses

FROM scratch

LABEL org.opencontainers.image.title="Kafka Connect Healer" \
      org.opencontainers.image.description="Restarts failed connectors and tasks in Kafka Connect clusters" \
      org.opencontainers.image.vendor="Entropic Works, Inc." \
      org.opencontainers.image.source="https://github.com/Entropic-Works/kafka-connect-healer" \
      org.opencontainers.image.url="https://github.com/Entropic-Works/kafka-connect-healer" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/kafka-connect-healer /kafka-connect-healer
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/go/LICENSE /licenses/go-LICENSE
COPY --from=build /out/licenses/ /licenses/dependencies/
COPY LICENSE /licenses/kafka-connect-healer-LICENSE
COPY NOTICE /licenses/kafka-connect-healer-NOTICE

USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/kafka-connect-healer"]
