import os
import json
import pytest
from pathlib import Path

import pipeline_tools.shared.submission.create_links as cl


@pytest.fixture(scope='module')
def test_data():
    class Data:
        project_level = True
        project_id = 'heart_1k_test_v2_S1_L001'
        file_name_string = 'heart_1k_test_v2_S1_L001'
        workspace_version = '2021-05-24T12:00:00.000000Z'
        project_level_project_id = '16ed4ad8-7319-46b2-8859-6fe1c1d73a82'
        input_uuids = ['heart_1k_test_v2_S1_L001_R1_001.fastq.gz', 'heart_1k_test_v2_S1_L001_R2_001.fastq.gz']
        output_file_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/outputs.json'
        project_level_file_name_string = "project=16ed4ad8-7319-46b2-8859-6fe1c1d73a82;library=10X 3' v2 sequencing;species=Homo sapiens;organ=kidney"
        project_level_input_uuids = [f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/hca_adapter_testing.input_metadata.json']
        analysis_process_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/analysis_process/151fe264-c670-4c77-a47c-530ff6b3127b_2021-05-24T12:00:00.000000Z.json'
        analysis_protocol_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/analysis_protocol/f2cdb4e5-b439-5cdf-ac41-161ff39d5790_2021-05-24T12:00:00.000000Z.json'
        project_level_output_file_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/metadata/analysis_file/a13d652a-468a-541c-bb03-8fb521421fbd_2021-05-24T12:00:00.000000Z.json'
        project_level_analysis_process_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/metadata/analysis_process/7f6c3249-2d24-407d-966f-411d84fbeba8_2021-05-24T12:00:00.000000Z.json'
        project_level_analysis_protocol_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/metadata/analysis_protocol/432a7422-59b5-5c46-8983-a7953f196781_2021-05-24T12:00:00.000000Z.json'
    return Data


class TestCreateLinks(object):
    def test_build_links_file(self, test_data):
        links_file_json = cl.test_build_links_file(
            test_data.project_id,
            test_data.input_uuids,
            test_data.output_file_path,
            test_data.file_name_string,
            test_data.workspace_version,
            test_data.analysis_process_path,
            test_data.analysis_protocol_path
        )

        desired_output_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/links-output.json'

        with open(desired_output_path) as f:
            desired_output = json.load(f)

        assert links_file_json == desired_output


class TestCreateProjectLevelLinks(object):
    def test_build_links_file(self, test_data):
        links_file_json = cl.test_build_links_file(
            test_data.project_level_project_id,
            test_data.project_level_input_uuids,
            test_data.project_level_output_file_path,
            test_data.project_level_file_name_string,
            test_data.workspace_version,
            test_data.project_level_analysis_process_path,
            test_data.project_level_analysis_protocol_path,
            test_data.project_level
        )

        desired_output_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/project-level/links/a09fa8a7-abde-580c-92a9-4d381af7f873_2021-05-24T12:00:00.000000Z_16ed4ad8-7319-46b2-8859-6fe1c1d73a82.json'

        with open(desired_output_path) as f:
            desired_output = json.load(f)

        assert links_file_json == desired_output
