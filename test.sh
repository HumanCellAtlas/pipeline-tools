#!/usr/bin/env bash

# Runs unit test or integration test suite, depending on value of TEST_SUITE.
#
# Usage: bash test.sh
#
# If TEST_SUITE == unit, this will run unit tests.
# If TEST_SUITE == schema, this will run "latest_schema" tests, which will attempt
# to run functions against the latest example files from the metadata-schema repo.
#
# Using an environment variable here instead of an argument to the script
# for ease of use with Travis CI.

# Set TEST_SUITE to "unit" by default, for convenience when testing locally.
if [ -z "$TEST_SUITE" ]; then
  printf "\nTEST_SUITE not specified. Defaulting to 'unit'.\n"
  TEST_SUITE="unit"
fi

# For integration tests, copy down latest metadata file(s) to test against.
if [ "$TEST_SUITE" = "latest_schema" ]; then
  PYTEST_SUITE="latest_schema"
  if [ ! -d pipeline_tools/tests/data/metadata/latest ]; then
    mkdir pipeline_tools/tests/data/metadata/latest
  fi
  git clone https://github.com/HumanCellAtlas/metadata-schema.git pipeline_tools/tests/data/metadata-schema
  cd pipeline_tools/tests/data/metadata-schema
  git checkout develop
  cd -
  LATEST=$(python pipeline_tools/tests/get_latest_schema_example_version.py -d pipeline_tools/tests/data/metadata-schema/examples/bundles)
  cp pipeline_tools/tests/data/metadata-schema/examples/bundles/$LATEST/Q4DemoSS2/files.json pipeline_tools/tests/data/metadata/latest/ss2_files.json
elif [ "$TEST_SUITE" = "unit" ]; then
  # Define unit tests to be anything not marked as "latest_schema"
  PYTEST_SUITE="not latest_schema"
else
  printf "\nTEST_SUITE value $TEST_SUITE not allowed. Must be 'unit' or 'latest_schema'.\n\n"
  exit 1
fi

pytest -v -m "$PYTEST_SUITE"
