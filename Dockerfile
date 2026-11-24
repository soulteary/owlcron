FROM oven/bun:1.4.5@sha256:3aa040e5445efa09d75bb428a64ba50db13e2b3ac9b6b0c7f145615f62b2ee18 AS frontend
WORKDIR /src
COPY htdocs-deps/package.json htdocs-deps/bun.lock htdocs-deps/bunfig.toml htdocs-deps/
RUN bun install --cwd htdocs-deps --frozen-lockfile
COPY htdocs-deps/ htdocs-deps/
COPY htdocs/ htdocs/
RUN bun run --cwd htdocs-deps build \
    && mkdir -p /out/htdocs \
    && cp -aL htdocs/. /out/htdocs/

FROM golang:1.27.4-trixie@sha256:430985542100b871e108ccc00e8dcc5eef08ca14ee5825e98ebf563d8ffec4ee AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/owlcron-engine . \
    && CGO_ENABLED=0 go build -trimpath -o /out/owlcron-script ./cmd/owlcron-script \
    && cd bin/echo-plugin && CGO_ENABLED=0 go build -trimpath -o /out/echo-plugin .

FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata procps passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 owlcron \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin owlcron \
    && mkdir -p /var/lib/owlcron /etc/owlcron /opt/owlcron/bin/echo-plugin \
    && chown 10001:10001 /var/lib/owlcron
WORKDIR /opt/owlcron
COPY --from=build /out/owlcron-engine ./owlcron-engine
COPY --from=build /out/owlcron-script ./owlcron-script
COPY --from=build /out/echo-plugin ./bin/echo-plugin/echo-plugin
COPY --from=frontend /out/htdocs/ ./htdocs/
COPY deploy/container-config.json /etc/owlcron/config.json
COPY LICENSE THIRD_PARTY_NOTICES.md /usr/share/licenses/owlcron/
COPY licenses/ /usr/share/licenses/owlcron/third-party/
USER 10001:10001
EXPOSE 3012
STOPSIGNAL SIGTERM
ENTRYPOINT ["/opt/owlcron/owlcron-engine", "--config", "/etc/owlcron/config.json"]
