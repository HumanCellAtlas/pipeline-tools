FROM python:3.6

LABEL maintainer="Mint Team <mintteam@broadinstitute.org>" \
  software="Python" \
  description="Python3 library used for processing notifications from HCA DCP and doing submissions."

RUN mkdir /tools

WORKDIR /tools

COPY . .

# Get latest setuptools because metadata-api installation fails without at least 40.1.0
RUN pip install -U setuptools

RUN pip install . --trusted-host github.com

# Install gsutil for crc32c hash calculation
RUN curl -sSL https://sdk.cloud.google.com | bash

ENV PATH $PATH:/root/google-cloud-sdk/bin
