#!/bin/bash
set -e 

SIZE="1000"
CRC32C="0b83b575"
PIPELINE_TYPE="Optimus"
PIPELINE_VERSION="optimus_v4.2.3"
GENUS_SPECIES="Mus musculus"
REFERENCE_VERSION="GencodeM21"
NCBI_TAXON_ID=10090
ASSEMBLY_TYPE="primary assembly"
REFERNECE_TYPE="genome sequence"
SHA256="12998c017066eb0d2a70b94e6ed3192985855ce390f321bbdb832022888bd251"

CREATION_TIME="22021-01-13T17:53:12.000000Z"
WORKSPACE_VERSION="2021-05-24T12:00:00.000000Z"

INPUT_UUID="heart_1k_test_v2_S1_L001"
PROJECT_INPUT_UUID="project=16ed4ad8-7319-46b2-8859-6fe1c1d73a82;library=10X 3 v2 sequencing;species=Mus musculus;organ=heart"

ANALYSIS_PROCESS_PATH=($(pwd)/analysis_process/**)
ANALYSIS_PROTOCOL_PATH=($(pwd)/analysis_protocol/**)

OUTPUT_FILE_PATH="$(pwd)/outputs.json"
OUTPUT_FILE_PATH_RENAME="$(pwd)/outputs-intermediate.json"

PROCESS_ID_INTERMEDIATE="151fe264-c670-4c77-a47c-530ff6b3127b"
PROCESS_ID_PROJECT="16ed4ad8-7319-46b2-8859-6fe1c1d73a82"

INTERMEDIATE_RUN_OUTPUT_FILES=(heart_1k_test_v2_S1_L001.bam heart_1k_test_v2_S1_L001.loom)
REFERENCE_FASTA="gs://hca-dcp-mint-test-data/yanc-test/GRCm38.primary_assembly.genome.fa"

METADATA_JSON_PROJECT="$(pwd)/pipeline_tools/tests/updated-data/staging/project-level/metadata.json"
LINKS_INPUTS_INTERMEDIATE="heart_1k_test_v2_S1_L001_R1_001.fastq.gz heart_1k_test_v2_S1_L001_R2_001.fastq.gz"
METADATA_JSON_INTERMEDIATE="$(pwd)/pipeline_tools/tests/updated-data/staging/intermediate-level/metadata.json"

PROJECT_LEVEL_LOOM="dummy-project-level.loom"


# INTERMEDIATE_LEVEL

# Create intermediate analysis file
python3 pipeline_tools/shared/submission/create_analysis_file.py \
  --input_uuid $INPUT_UUID \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --input_file $METADATA_JSON_INTERMEDIATE

# Create intermediate process file
python3 pipeline_tools/shared/submission/create_analysis_process.py \
  --input_uuid $INPUT_UUID \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --input_file $METADATA_JSON_INTERMEDIATE \
  --references $REFERENCE_FASTA

# Create intermediate protocol file
python3 pipeline_tools/shared/submission/create_analysis_protocol.py \
  --input_uuid $INPUT_UUID \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --pipeline_version $PIPELINE_VERSION

# Create intermediate bam/loom descriptor files
for f in "${INTERMEDIATE_RUN_OUTPUT_FILES[@]}"
do
  python3 pipeline_tools/shared/submission/create_file_descriptor.py \
    --size $SIZE \
    --sha256 $SHA256 \
    --crc32c $CRC32C \
    --pipeline_type "Optimus" \
    --file_path $f \
    --input_uuid $INPUT_UUID \
    --creation_time $CREATION_TIME \
    --workspace_version $WORKSPACE_VERSION
done

# Create intermediate links file
python3 pipeline_tools/shared/submission/create_links.py \
  --project_id $PROCESS_ID_INTERMEDIATE \
  --input_uuids $LINKS_INPUTS_INTERMEDIATE \
  --analysis_process_path $ANALYSIS_PROCESS_PATH \
  --analysis_protocol_path $ANALYSIS_PROTOCOL_PATH \
  --workspace_version $WORKSPACE_VERSION \
  --output_file_path $OUTPUT_FILE_PATH \
  --file_name_string $INPUT_UUID


# Create reference file metadata
python3 pipeline_tools/shared/submission/create_reference_file.py \
  --genus_species "$GENUS_SPECIES" \
  --file_path $REFERENCE_FASTA \
  --workspace_version $WORKSPACE_VERSION \
  --input_uuid $REFERENCE_FASTA \
  --reference_version $REFERENCE_VERSION \
  --ncbi_taxon_id $NCBI_TAXON_ID \
  --pipeline_type "Optimus" \
  --assembly_type "$ASSEMBLY_TYPE" \
  --reference_type "$REFERNECE_TYPE"

# Create reference file descriptor
python3 pipeline_tools/shared/submission/create_file_descriptor.py \
  --size $SIZE \
  --sha256 $SHA256 \
  --crc32c $CRC32C \
  --pipeline_type "Optimus" \
  --file_path $REFERENCE_FASTA \
  --input_uuid $REFERENCE_FASTA \
  --creation_time $CREATION_TIME \
  --workspace_version $WORKSPACE_VERSION

# Rename the output file so we don't overwrite on project run
mv $OUTPUT_FILE_PATH $OUTPUT_FILE_PATH_RENAME

mv $(pwd)/analysis_files/ $(pwd)/analysis_file-intermediate/
mv $(pwd)/analysis_protocol/ $(pwd)/analysis_protocol-intermediate/
mv $(pwd)/analysis_process/ $(pwd)/analysis_process-intermediate/
mv $(pwd)/links/ $(pwd)/links-intermediate/




# PROJECT_LEVEL 

# Create project analysis file
python3 pipeline_tools/shared/submission/create_analysis_file.py \
  --project_level true \
  --input_uuid "$PROJECT_INPUT_UUID" \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --input_file $PROJECT_LEVEL_LOOM

# Create project process file
python3 pipeline_tools/shared/submission/create_analysis_process.py \
  --project_level true \
  --input_uuid "$PROJECT_INPUT_UUID" \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --input_file $METADATA_JSON_PROJECT \
  --references $REFERENCE_FASTA

# Create project protocol file
python3 pipeline_tools/shared/submission/create_analysis_protocol.py \
  --project_level true \
  --input_uuid "$PROJECT_INPUT_UUID" \
  --pipeline_type "Optimus" \
  --workspace_version $WORKSPACE_VERSION \
  --pipeline_version $PIPELINE_VERSION

# Create project file descriptor 
python3 pipeline_tools/shared/submission/create_file_descriptor.py \
    --size $SIZE \
    --sha256 $SHA256 \
    --crc32c $CRC32C \
    --pipeline_type "Optimus" \
    --file_path $PROJECT_LEVEL_LOOM \
    --input_uuid "$PROJECT_INPUT_UUID" \
    --creation_time $CREATION_TIME \
    --workspace_version $WORKSPACE_VERSION

# Create intermediate links file
python3 pipeline_tools/shared/submission/create_links.py \
  --project_level true \
  --project_id $PROCESS_ID_PROJECT \
  --input_uuids "heart_1k_test_v2_S1_L001.loom" \
  --analysis_process_path $ANALYSIS_PROCESS_PATH \
  --analysis_protocol_path $ANALYSIS_PROTOCOL_PATH \
  --workspace_version $WORKSPACE_VERSION \
  --output_file_path $OUTPUT_FILE_PATH \
  --file_name_string "$PROJECT_INPUT_UUID" # We should change the naming here, its confusing