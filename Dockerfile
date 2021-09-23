FROM python:3.6.11

LABEL maintainer="Mint Team <mintteam@broadinstitute.org>" \
  software="Python" \
  description="Python3 library used for processing notifications from HCA DCP and doing submissions."

RUN mkdir /tools

WORKDIR /tools

COPY . .

RUN apt-get update && apt-get -y install jq

# Get latest setuptools because metadata-api installation fails without at least 40.1.0
RUN pip install -U setuptools

RUN pip install . --trusted-host github.com

# Install gsutil to get crc32c and file size from cloud data
RUN curl -sSL https://sdk.cloud.google.com | bash

ENV PATH $PATH:/root/google-cloud-sdk/bin
