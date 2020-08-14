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

# Instal crc32 for hash calculation
RUN apt-get update && apt-get install libarchive-zip-perl