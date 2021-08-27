FROM ghcr.io/bluesky/tiled:main as base

COPY . .
RUN pip install -r requirements.txt
ENTRYPOINT ["/deploy/entrypoint.sh"]
