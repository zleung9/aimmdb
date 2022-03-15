FROM ghcr.io/bluesky/tiled:main as base

FROM base as builder

WORKDIR /build
COPY aimmdb .
RUN pip install .

FROM base as app
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY deploy/spin/docker/tiled/gunicorn_config.py /deploy/spin/docker/tiled/entrypoint.sh .
COPY deploy/spin/docker/tiled/static/ /opt/venv/share/tiled/static

ENTRYPOINT ["/deploy/entrypoint.sh"]
