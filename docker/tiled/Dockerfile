FROM ghcr.io/bluesky/tiled:main as base

COPY feff_xas_tree.py requirements.txt /deploy/
COPY config.yml /deploy/config/
ENV TILED_CONFIG=/deploy/config
RUN pip install -r requirements.txt
