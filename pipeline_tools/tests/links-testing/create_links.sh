#!/bin/bash

python3 create-links \
  --output_file_path "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/outputs.json" \
  --project_id "78b2406d-bff2-46fc-8b61-20690e602227" \
  --workspace_version "2021-09-13T21:40:05.000000Z" \
  --input_uuids "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/input_ids.json" \
  --input_uuids_path "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/input_ids.json" \
  --analysis_process_path "glob-8cd20462bc32c0d31b1ab9f74947cec7/eba45af0-cc36-4c65-b1ae-e7fd4f55afb0_2021-09-13T21:40:05.000000Z.json" \
  --analysis_protocol_path "glob-8cd20462bc32c0d31b1ab9f74947cec7/a0c6dd38-c465-5c98-bec0-d4acc341fca5_2021-09-13T21:40:05.000000Z.json" \
  --analysis_process_list_path "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/process_list.json"  \
  --analysis_protocol_list_path "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/protocol_list.json" \
  --ss2_bam "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/ss2_bam.json" \
  --ss2_bai "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/ss2_bai.json" \
  --ss2_fastq1 "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/ss2_fastq1.json" \
  --ss2_fastq2 "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/ss2_fastq2.json" \
  --file_name_string "project=78b2406d-bff2-46fc-8b61-20690e602227;library=Smart-seq2;species=Homo sapiens;organ=pancreas" \
  --pipeline_type "SS2" \
  --project_level true 