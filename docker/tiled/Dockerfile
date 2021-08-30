FROM ghcr.io/bluesky/tiled:main as base

COPY gunicorn_config.py tree.py requirements.txt entrypoint.sh /deploy/
COPY config.yml /deploy/config/
ENV TILED_CONFIG=/deploy/config
RUN pip install -r requirements.txt

# need to change permissions so we can modify /deploy in the entrypoint with nersc uid/gid
RUN chown -R 79966:94721 .

ENTRYPOINT ["/deploy/entrypoint.sh"]
