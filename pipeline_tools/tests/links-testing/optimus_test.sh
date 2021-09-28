#!/bin/bash

python3 create_links.py \
    --project_id "f48e7c39-cc67-4055-9d79-bc437892840c" \
    --input_uuids 8c0658d5-dcb7-4408-b5b1-b277a6b9a27f df6cd035-cbdf-4b67-9076-49b29b2d2047 \
    --output_file_path "/Users/wdingman/go/src/github.com/HumanCellAtlas/pipeline-tools/pipeline_tools/shared/submission/outputs.json" \
    --workspace_version "2021-09-10T15:13:09.000000Z" \
    --analysis_process_path "/cromwell_root/fc-8d38952b-e18d-498d-8db1-948736d960e0/b053faf8-b9e5-4e7f-86fe-63ad51fbdf08/CreateOptimusAdapterMetadata/3bb49768-dbc0-4839-907a-5baa5ce2919b/call-CreateIntermediateOptimusAdapters/shard-1/CreateOptimusAdapterObjects/4a5c18bf-6e8c-4db4-b1b9-09ce7c34d429/call-GetAnalysisProcessMetadata/cacheCopy/glob-60fac9ee177ed8b542495afc423626bd/44519294-af78-4898-a51c-13ca11e02879_2021-09-10T15:13:09.000000Z.json" \
    --analysis_protocol_path "/cromwell_root/fc-8d38952b-e18d-498d-8db1-948736d960e0/b053faf8-b9e5-4e7f-86fe-63ad51fbdf08/CreateOptimusAdapterMetadata/3bb49768-dbc0-4839-907a-5baa5ce2919b/call-CreateIntermediateOptimusAdapters/shard-1/CreateOptimusAdapterObjects/4a5c18bf-6e8c-4db4-b1b9-09ce7c34d429/call-GetAnalysisProtocolMetadata/cacheCopy/glob-60fac9ee177ed8b542495afc423626bd/61223a2e-a775-53f4-8aab-fc3b4ef88722_2021-09-10T15:13:09.000000Z.json" \
    --file_name_string "6e4808a4-cfc8-463a-bb86-96e1cd371463" \
    --project_level false \
    --pipeline_type "Optimus"