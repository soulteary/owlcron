FROM node:22-bookworm-slim AS frontend
WORKDIR /src
COPY htdocs-deps/package*.json htdocs-deps/
RUN npm ci --prefix htdocs-deps
COPY htdocs-deps/ htdocs-deps/
COPY htdocs/ htdocs/
RUN npm run build --prefix htdocs-deps \
    && mkdir -p /out/htdocs \
    && cp -aL htdocs/. /out/htdocs/

FROM golang:1.27.3-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/owlcron-engine . \
    && CGO_ENABLED=0 go build -trimpath -o /out/owlcron-script ./cmd/owlcron-script \
    && cd bin/echo-plugin && CGO_ENABLED=0 go build -trimpath -o /out/echo-plugin .

FROM debian:trixie-slim
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
