FROM python:slim

LABEL maintainer="Vahidreza Naderi <vahidrnaderi@gmail.com>"

ENV SBACKUP_DB_URL='redis:6379' \
    SBACKUP_AWS_ENDPOINT_URL='http://minio:9000' \
    SBACKUP_AWS_SECRET_ACCESS_KEY='YOUR_AWS_SECRET_ACCESS_KEY' \
    SBACKUP_AWS_ACCESS_KEY_ID='YOUR_AWS_ACCESS_KEY_ID' \
    SBACKUP_AWS_DEFAULT_REGION='None' \
    SBACKUP_DEST_AWS_ENDPOINT_URL='http://minio:9000' \
    SBACKUP_DEST_AWS_SECRET_ACCESS_KEY='YOUR_DEST_AWS_SECRET_ACCESS_KEY' \
    SBACKUP_DEST_AWS_ACCESS_KEY_ID='YOUR_DEST_AWS_ACCESS_KEY_ID' \
    SBACKUP_DEST_AWS_DEFAULT_REGION='None'

WORKDIR /opt/src

COPY . .

RUN pip install --upgrade pip && \
    pip install setuptools-scm>=8.0 && \
    apt-get update && \
    apt-get install -y git && \
    pip install .

ENTRYPOINT ["/bin/bash"] 



