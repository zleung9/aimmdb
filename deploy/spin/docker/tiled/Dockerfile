FROM ghcr.io/bluesky/tiled:main as base

COPY gunicorn_config.py tree.py requirements.txt entrypoint.sh /deploy/
COPY config.yml /deploy/config/
ENV TILED_CONFIG=/deploy/config
RUN pip install -r requirements.txt

ENTRYPOINT ["/deploy/entrypoint.sh"]
