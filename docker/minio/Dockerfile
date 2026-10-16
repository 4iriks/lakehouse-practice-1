FROM golang:1.24.7-bookworm AS build
ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
COPY minio-source.tar.gz /tmp/minio.tar.gz
RUN mkdir /src && tar -xzf /tmp/minio.tar.gz -C /src --strip-components=1
WORKDIR /src
ENV CGO_ENABLED=0 GOMAXPROCS=2
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go build -p 2 -trimpath -o /minio .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
