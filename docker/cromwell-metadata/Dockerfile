FROM quay.io/humancellatlas/secondary-analysis-pipeline-tools:cw-get-pipeline-versions-from-wdls-than-cromwell-labels

RUN mkdir /cromwell-metadata
WORKDIR /cromwell-metadata

COPY cromwell_credentials.txt .

COPY caas_key.json .
