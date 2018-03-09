import unittest
import json
import os
import pytest
from pipeline_tools import input_utils
from pipeline_tools import dcp_utils


class TestInputUtils(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        with open(self.data_file('metadata/v4/ss2_assay.json')) as f:
            self.ss2_assay_json_v4 = json.load(f)
        with open(self.data_file('metadata/v4/ss2_manifest.json')) as f:
            self.ss2_manifest_json_v4 = json.load(f)
            self.ss2_manifest_files_v4 = dcp_utils.get_manifest_file_dicts(self.ss2_manifest_json_v4)
        with open(self.data_file('metadata/v5/ss2_manifest_stub.json')) as f:
            self.ss2_manifest_json_v5 = json.load(f)
            self.ss2_manifest_files_v5 = dcp_utils.get_manifest_file_dicts(self.ss2_manifest_json_v5)
        with open(self.data_file('metadata/v5/ss2_files.json')) as f:
            self.ss2_files_json_v5 = json.load(f)
        with open(self.data_file('metadata/v4/optimus_assay.json')) as f:
            self.optimus_assay_json_v4 = json.load(f)
        with open(self.data_file('metadata/v4/optimus_manifest.json')) as f:
            self.optimus_manifest_json_v4 = json.load(f)

    def test_get_sample_id_default_version(self):
        sample_id = input_utils.get_sample_id(self.ss2_assay_json_v4)
        self.assertEqual(sample_id, 'b0c57b9c-860b-4bbf-84aa-5f845508101d')

    def test_get_sample_id_v4(self):
        sample_id = input_utils.get_sample_id(self.ss2_assay_json_v4, '4')
        self.assertEqual(sample_id, 'b0c57b9c-860b-4bbf-84aa-5f845508101d')

    def test_get_sample_id_v5_raises_error(self):
        with self.assertRaises(NotImplementedError):
            input_utils.get_sample_id(self.ss2_assay_json_v4, '5')

    def test_get_sample_id_non_existent_version_raises_error(self):
        with self.assertRaises(NotImplementedError):
            input_utils.get_sample_id(self.ss2_assay_json_v4, '-1')

    def test_get_input_metadata_file_uuid_default_version(self):
        uuid = input_utils.get_input_metadata_file_uuid(self.ss2_manifest_files_v4)
        self.assertEqual(uuid, 'e56638c7-f026-42d0-9be8-24b71a7d6e86')

    def test_get_input_metadata_file_uuid_v4(self):
        uuid = input_utils.get_input_metadata_file_uuid(self.ss2_manifest_files_v4, '4')
        self.assertEqual(uuid, 'e56638c7-f026-42d0-9be8-24b71a7d6e86')

    def test_get_input_metadata_file_uuid_v5(self):
        uuid = input_utils.get_input_metadata_file_uuid(self.ss2_manifest_files_v5, '5')
        self.assertEqual(uuid, '851d312b-9d16-49d4-a0de-4d7def69d126')

    def test_get_smart_seq_2_fastq_names_default_version(self):
        fastq_1_name, fastq_2_name = input_utils.get_smart_seq_2_fastq_names(self.ss2_assay_json_v4)
        self.assertEqual(fastq_1_name, 'R1.fastq.gz')
        self.assertEqual(fastq_2_name, 'R2.fastq.gz')

    def test_get_smart_seq_2_fastq_names_v4(self):
        fastq_1_name, fastq_2_name = input_utils.get_smart_seq_2_fastq_names(self.ss2_assay_json_v4, '4')
        self.assertEqual(fastq_1_name, 'R1.fastq.gz')
        self.assertEqual(fastq_2_name, 'R2.fastq.gz')

    def test_get_smart_seq_2_fastq_names_v5(self):
        fastq_1_name, fastq_2_name = input_utils.get_smart_seq_2_fastq_names(self.ss2_files_json_v5, '5')
        self.assertEqual(fastq_1_name, 'R1.fastq.gz')
        self.assertEqual(fastq_2_name, 'R2.fastq.gz')

    @pytest.mark.latest_schema
    def test_get_smart_seq_2_fastq_names_latest(self):
        with open(self.data_file('metadata/latest/ss2_files.json')) as f:
            ss2_files_json_latest = json.load(f)
        fastq_1_name, fastq_2_name = input_utils.get_smart_seq_2_fastq_names(ss2_files_json_latest, '5')
        self.assertEqual(fastq_1_name, 'R1.fastq.gz')
        self.assertEqual(fastq_2_name, 'R2.fastq.gz')

    def test_get_optimus_lanes_default_version(self):
        lanes = input_utils.get_optimus_lanes(self.optimus_assay_json_v4)
        self.assertEqual(len(lanes), 2)
        self.assertEqual(lanes[0]['r1'], 'pbmc8k_S1_L007_R1_001.fastq.gz')
        self.assertEqual(lanes[0]['r2'], 'pbmc8k_S1_L007_R2_001.fastq.gz')
        self.assertEqual(lanes[0]['i1'], 'pbmc8k_S1_L007_I1_001.fastq.gz')
        self.assertEqual(lanes[1]['r1'], 'pbmc8k_S1_L008_R1_001.fastq.gz')
        self.assertEqual(lanes[1]['r2'], 'pbmc8k_S1_L008_R2_001.fastq.gz')
        self.assertEqual(lanes[1]['i1'], 'pbmc8k_S1_L008_I1_001.fastq.gz')

    def test_get_optimus_lanes_v4(self):
        lanes = input_utils.get_optimus_lanes(self.optimus_assay_json_v4, '4')
        self.assertEqual(len(lanes), 2)
        self.assertEqual(lanes[0]['r1'], 'pbmc8k_S1_L007_R1_001.fastq.gz')
        self.assertEqual(lanes[0]['r2'], 'pbmc8k_S1_L007_R2_001.fastq.gz')
        self.assertEqual(lanes[0]['i1'], 'pbmc8k_S1_L007_I1_001.fastq.gz')
        self.assertEqual(lanes[1]['r1'], 'pbmc8k_S1_L008_R1_001.fastq.gz')
        self.assertEqual(lanes[1]['r2'], 'pbmc8k_S1_L008_R2_001.fastq.gz')
        self.assertEqual(lanes[1]['i1'], 'pbmc8k_S1_L008_I1_001.fastq.gz')

    def test_get_optimus_lanes_v5(self):
        with self.assertRaises(NotImplementedError):
            input_utils.get_optimus_lanes(self.optimus_assay_json_v4, '5')

    def test_get_optimus_inputs(self):
        lanes = input_utils.get_optimus_lanes(self.optimus_assay_json_v4)
        manifest_files = dcp_utils.get_manifest_file_dicts(self.optimus_manifest_json_v4)
        r1, r2, i1 = input_utils.get_optimus_inputs(lanes, manifest_files)

        expected_r1 = ['gs://foo/L7_R1.fastq.gz', 'gs://foo/L8_R1.fastq.gz']
        expected_r2 = ['gs://foo/L7_R2.fastq.gz', 'gs://foo/L8_R2.fastq.gz']
        expected_i1 = ['gs://foo/L7_I1.fastq.gz', 'gs://foo/L8_I1.fastq.gz']

        self.assertEqual(r1, expected_r1)
        self.assertEqual(r2, expected_r2)
        self.assertEqual(i1, expected_i1)

    @staticmethod
    def data_file(file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name

if __name__ == '__main__':
    unittest.main()
