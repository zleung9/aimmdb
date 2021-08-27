FROM ubuntu:latest as base

MAINTAINER Joseph Kleinhenz <jkleinhenz@lbl.gov>

# general environment for docker
ENV DEBIAN_FRONTEND=noninteractive

# setup timezone data
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# enable sudo
RUN apt-get update && apt-get install -y --no-install-recommends sudo

# create docker user
RUN useradd -m -s /bin/bash -u 999 docker && echo "docker:docker" | chpasswd && adduser docker sudo

# enable passwordless sudo
RUN echo "docker ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/docker

USER    docker
WORKDIR /home/docker

RUN sudo apt-get install -y gnupg wget

RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -

RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list

RUN sudo apt-get update

RUN sudo apt-get install -y mongodb-org
