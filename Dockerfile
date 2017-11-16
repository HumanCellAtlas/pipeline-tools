FROM python:2.7

LABEL maintainer="Mint Team <mintteam@broadinstitute.org>" \
  software="Python" \
  description="Python2.7 library used for processing notifications from HCA DCP and doing submissions."

RUN mkdir /tools

WORKDIR /tools

COPY . .

RUN pip install .

# Install the latest hca-cli for submit.wdl
RUN pip install hca
