FROM quay.io/humancellatlas/secondary-analysis-pipeline-tools:v0.25.0

RUN mkdir /cromwell-metadata
WORKDIR /cromwell-metadata

COPY cromwell_credentials.txt .

COPY caas_key.json .
