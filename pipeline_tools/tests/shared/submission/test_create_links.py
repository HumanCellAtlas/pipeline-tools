import os
import json
import pytest
from pathlib import Path

import pipeline_tools.shared.submission.create_links as cl


@pytest.fixture(scope='module')
def test_data():
    class Data:
        project_id = "heart_1k_test_v2_S1_L001"
        file_name_string = "heart_1k_test_v2_S1_L001"
        input_uuids = ["heart_1k_test_v2_S1_L001_R1_001.fastq.gz", "heart_1k_test_v2_S1_L001_R2_001.fastq.gz"]
        analysis_process_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/analysis_process/151fe264-c670-4c77-a47c-530ff6b3127b_2021-05-24T12:00:00.000000Z.json'
        analysis_protocol_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/analysis_protocol/f2cdb4e5-b439-5cdf-ac41-161ff39d5790_2021-05-24T12:00:00.000000Z.json'
        workspace_version = "2021-05-24T12:00:00.000000Z"
        output_file_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/analysis_file/output.json'

    return Data


class TestCreateLinks(object):
    def test_build_links_file(self, test_data):
        links_file_json = cl.test_build_links_file(
            test_data.project_id,
            test_data.input_uuids,
            test_data.file_name_string,
            test_data.output_file_path,
            test_data.workspace_version,
            test_data.analysis_process_path,
            test_data.analysis_protocol_path
        )

        desired_output_path = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/links-output.json'

        with open(desired_output_path) as f:
            desired_output = json.load(f)

        assert links_file_json == desired_output
