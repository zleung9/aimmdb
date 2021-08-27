FROM ghcr.io/bluesky/tiled:main as base

COPY . .
RUN pip install -r requirements.txt

# we need to change permissions so we can modify config.yml in the entrypoint with nersc uid/gid
RUN chown 79966:94721 config.yml

ENTRYPOINT ["/deploy/entrypoint.sh"]
