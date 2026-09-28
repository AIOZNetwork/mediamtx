# The AIOZ Stream livestream image, built from this checkout.
#
# Same runtime layout as aioz-stream's deploy/docker/live.Dockerfile, so it is a
# drop-in for the `live` slots of aioz-stream's docker-compose.deploy.yml:
#
#   /app/livestream     the binary (CMD)
#   /app/aioz-live.yml  the config, found through defaultConfPaths in
#                       internal/core/core.go; mount it, it is not baked in
#   /app/streamId, /app/input-live   runtime state, usually bind mounts
#
# Build (from the repository root). gitlab.internal/aioz-depin/go-sdk is a
# private module, so the build needs a route to gitlab.internal and one of two
# credentials, both optional mounts:
#
#   # locally: the ssh-agent (the key must be loaded with ssh-add)
#   docker buildx build -f docker/aioz-live.Dockerfile \
#     --ssh default --add-host gitlab.internal=10.0.0.50 \
#     --build-arg RELEASE=v1.21.0-aioz -t livestream:dev --load .
#
#   # CI: a netrc with a job token, as aioz-stream's pipeline mounts it
#   ... --secret id=netrc,src=/tmp/netrc ...

ARG GO_IMAGE=golang:1.26-bookworm

# ---------------------------------------------------------------------------
# The binary.
# ---------------------------------------------------------------------------
FROM ${GO_IMAGE} AS build

# GOINSECURE: Go discovers the repository with its own HTTPS request to
# gitlab.internal, which this image cannot verify (internal CA). go.sum still
# pins every module hash, so integrity does not depend on TLS.
# hadolint ignore=DL3064
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPRIVATE=gitlab.internal/* \
    GOINSECURE=gitlab.internal/*

WORKDIR /src/mediamtx

# Modules first, so a source change does not refetch them. git over ssh is
# used only when an agent is forwarded; otherwise https with the netrc.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=secret,id=netrc,target=/root/.netrc \
    --mount=type=ssh \
    set -eu; \
    if [ -S "${SSH_AUTH_SOCK:-}" ]; then \
        git config --global url."git@gitlab.internal:".insteadOf "https://gitlab.internal/"; \
        export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"; \
    fi; \
    go mod download

COPY . ./

# Two embedded files are generated and gitignored (VERSION, hls.min.js), so
# `go build` alone fails on a clean checkout. The rpicamera generator is
# skipped: its embeds are behind `linux && (arm || arm64)`.
#
# RELEASE, when set, is the version `mediamtx --version` prints. Otherwise
# versiongetter reads it from .git (kept in the context by .dockerignore) and
# falls back to v0.0.0 when there is none.
ARG RELEASE=
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    if [ -n "${RELEASE}" ]; then \
        printf '%s' "${RELEASE}" > internal/core/VERSION; \
    else \
        go generate ./internal/core; \
    fi; \
    go generate ./internal/servers/hls; \
    test -s internal/servers/hls/hls.min.js || { \
        echo "hls.min.js was not generated"; exit 1; \
    }; \
    go build -trimpath -o /out/livestream .

# ---------------------------------------------------------------------------
# The image that ships.
# ---------------------------------------------------------------------------
FROM ubuntu:22.04

ENV TZ=Asia/Ho_Chi_Minh \
    DEBIAN_FRONTEND=noninteractive

# ffmpeg: ABR transcoding (runOnReady). curl: the runOnConnect/runOnDisconnect
# webhooks. ca-certificates: the S3 and DePIN uploaders.
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    ffmpeg \
    ca-certificates \
    tzdata && \
    rm -rf /var/lib/apt/lists/*

ENV APP_ENV=app

# The same fixed, unprivileged uid as aioz-stream's images: they share
# ./input, ./input-live and ./output as bind mounts.
ARG APP_UID=10001
RUN groupadd --system --gid "${APP_UID}" app && \
    useradd --system --uid "${APP_UID}" --gid app --home-dir /app --no-create-home app && \
    mkdir -p /app/streamId /app/input-live && \
    chown -R app:app /app

WORKDIR /app

COPY --from=build --chown=app:app /out/livestream .

USER app

# RTSP, RTMP, HLS, API, as aioz-stream's deploy/mediamtx/aioz-live.yml sets them.
EXPOSE 8554 1935 2327 9997

CMD ["/app/livestream"]
