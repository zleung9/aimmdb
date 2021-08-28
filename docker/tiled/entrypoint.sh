#!/usr/bin/env bash

cat /secrets/tiled-auth.yml >> config.yml
export TILED_CONFIG=/deploy/config.yml
/app/docker/check_config.py && exec gunicorn --config /app/docker/gunicorn_config.py
