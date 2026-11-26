FROM --platform=$BUILDPLATFORM golang:1.27.4 AS datapipe

# Same ML-KEM handshake failure as BuildKit, here against proxy.golang.org.
ENV GODEBUG=tlsmlkem=0

WORKDIR /src

COPY message_processor/go.mod message_processor/go.sum ./
RUN go mod download

COPY message_processor/ ./

ARG TARGETOS
ARG TARGETARCH

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -ldflags="-s -w" -o /out/data_pipe .

FROM python:3.14.7-slim-trixie AS python-base

FROM python-base AS python-deps
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY uv.lock pyproject.toml README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_LINK_MODE=copy uv sync --frozen --no-dev

FROM python-base AS os-deps
ARG TARGETARCH
ARG APT_REFRESH=local

RUN --mount=type=cache,id=apt-trixie-${TARGETARCH},target=/var/cache/apt,sharing=locked \
    --mount=type=cache,id=apt-lists-trixie-${TARGETARCH},target=/var/lib/apt/lists,sharing=locked \
    printf 'Refreshing Debian packages for build %s\n' "$APT_REFRESH" \
 && mv /etc/apt/apt.conf.d/docker-clean /tmp/docker-clean \
 && apt-get update --error-on=any \
 && apt-get upgrade -y \
 && apt-get install -y --no-install-recommends \
    postgresql-client \
    git \
    ca-certificates \
 && apt-get autoremove -y \
 && mv /tmp/docker-clean /etc/apt/apt.conf.d/docker-clean

FROM os-deps AS runtime

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

RUN rm -rf /var/lib/apt/lists/* \
           /usr/local/lib/python3.14/site-packages/pip* \
           /usr/local/lib/python3.14/ensurepip/_bundled \
           /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14 \
 && groupadd --system --gid 10001 pepeunit \
 && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/pepeunit pepeunit \
 && mkdir -p /app/tmp /app/repo_cache /app/prometheus_metrics \
 && chown -R pepeunit:pepeunit /app/tmp /app/repo_cache /app/prometheus_metrics

ENV PATH="/app/.venv/bin:$PATH" \
    UV_NO_SYNC=1

COPY --from=python-deps /bin/uv /bin/uv
COPY --link --from=python-deps /app/.venv /app/.venv
COPY --link . .
COPY --link --chmod=0755 entrypoint.sh ./entrypoint.sh
COPY --link --chmod=0755 --from=datapipe /out/data_pipe /app/bin/data_pipe

# The cache volume is prepared before starting the unprivileged application.
USER 10001:10001

CMD ["/bin/bash", "/app/entrypoint.sh"]
