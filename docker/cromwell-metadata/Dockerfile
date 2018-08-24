FROM quay.io/humancellatlas/secondary-analysis-pipeline-tools:13ee7ef14e3b

RUN mkdir /cromwell-metadata
WORKDIR /cromwell-metadata

COPY cromwell_credentials.txt .

COPY caas_key.json .
