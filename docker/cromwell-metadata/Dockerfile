FROM quay.io/humancellatlas/secondary-analysis-pipeline-tools:cw-update-to-new-bundle-structure

RUN mkdir /cromwell-metadata
WORKDIR /cromwell-metadata

COPY cromwell_credentials.txt .

COPY caas_key.json .
