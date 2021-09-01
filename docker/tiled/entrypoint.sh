#!/usr/bin/env bash

cp /secrets/tiled-auth.yml /deploy/config/
export PYTHONPATH=/deploy:$PYTHONPATH
#/app/docker/check_config.py && exec gunicorn --config /deploy/gunicorn_config.py
/app/docker/check_config.py && exec tiled serve config
