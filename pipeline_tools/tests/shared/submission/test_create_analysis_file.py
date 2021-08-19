import os
import json
import pytest

import pipeline_tools.shared.submission.create_analysis_file as caf
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        project_level_input_uuid = "project=16ed4ad8-7319-46b2-8859-6fe1c1d73a82;library=10X 3 v2 sequencing;species=Homo sapiens;organ=kidney"
        input_file = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/intermediate-level/metadata.json'
        project_level_input_file = 'hca_adapter_testing/hca_adapter_test/hca_adapter_testing.loom'
        project_level_pipeline_type = 'OptimusPostProcessing'
        workspace_version = '2021-05-24T12:00:00.000000Z'
        input_uuid = 'heart_1k_test_v2_S1_L001'
        pipeline_type = 'optimus'
        project_level = True

    return Data


@pytest.fixture
def data_file():
    def _data_file(file_name):
        return (
            f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/{file_name}'
        )

    return _data_file


class TestCreateAnalysisFile(object):
    def test_build_analysis_file(self, test_data):
        analysis_file_json = caf.test_build_analysis_file(
            input_uuid=test_data.input_uuid,
            input_file=test_data.input_file,
            pipeline_type=test_data.pipeline_type,
            workspace_version=test_data.workspace_version
        )

        desired_output_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/intermediate-level/outputs.json'

        with open(desired_output_path) as f:
            desired_output = json.load(f)

        assert analysis_file_json == desired_output


class TestCreateProjectLevelAnalysisFile(object):
    def test_build_analysis_file(self, test_data):
        analysis_file_json = caf.test_build_analysis_file(
            input_uuid=test_data.project_level_input_uuid,
            input_file=test_data.project_level_input_file,
            pipeline_type=test_data.project_level_pipeline_type,
            workspace_version=test_data.workspace_version,
            project_level=test_data.project_level
        )

        desired_output_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/outputs.json'

        with open(desired_output_path) as f:
            desired_output = json.load(f)

        assert analysis_file_json == desired_output
