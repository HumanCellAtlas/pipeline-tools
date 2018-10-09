import json
import os
import pytest
from copy import deepcopy

import pipeline_tools.create_analysis_metadata as cam


@pytest.fixture(scope='module')
def test_data():
    class Data:
        output_urls = {
            'gs://foo/bar/Aligned.sortedByCoord.out.bam',
            'gs://foo/bar/GSM1957573_rna_metrics'
        }
        extension_to_format = {
            '.bam': 'bam',
            '_metrics': 'metrics'
        }
        outputs = [
            {
                'describedBy': 'http://schema.humancellatlas.org/type/file/4.5.6/analysis_file',
                'schema_type': 'file',
                'file_core': {
                    'file_name': 'Aligned.sortedByCoord.out.bam',
                    'file_format': 'bam',
                    'checksum': '0123456789abcdef0123456789abcdef'
                }
            },
            {
                'describedBy': 'http://schema.humancellatlas.org/type/file/4.5.6/analysis_file',
                'schema_type': 'file',
                'file_core': {
                    'file_name': 'GSM1957573_rna_metrics',
                    'file_format': 'metrics',
                    'checksum': 'abcdef0123456789abcdef0123456789'
                }
            }
        ]
        inputs = [
            {
                'parameter_name': 'fastq_read1',
                'parameter_value': 'gs://foo/path/read1.fastq.gz',
                'checksum': '0123456789abcdef0123456789abcdef'
            },
            {
                'parameter_name': 'fastq_read2',
                'parameter_value': 'gs://foo/path/read2.fastq.gz',
                'checksum': 'abcdef0123456789abcdef0123456789'
            },
            {
                'parameter_name': 'output_prefix',
                'parameter_value': 'GSM1957573'
            },
            {
                'parameter_name': 'test_int',
                'parameter_value': '123'
            }
        ]
        input_url_to_md5 = {
            'gs://foo/path/read1.fastq.gz': '0123456789abcdef0123456789abcdef',
            'gs://foo/path/read2.fastq.gz': 'abcdef0123456789abcdef0123456789'
        }
        schema_url = 'http://schema.humancellatlas.org'

    return Data


@pytest.fixture
def data_file():
    def _data_file(file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name

    return _data_file


class TestCreateAnalysisMetadata(object):

    def test_create_analysis_process(self, test_data, data_file):
        analysis_process = cam.create_analysis_process(raw_schema_url=test_data.schema_url,
                                                       metadata_file=data_file('metadata.json'),
                                                       analysis_process_schema_version='1.2.3',
                                                       analysis_id='12345abcde',
                                                       input_bundles_string='foo_input_bundle1,foo_input_bundle2',
                                                       reference_bundle='foo_ref_bundle',
                                                       inputs=test_data.inputs,
                                                       outputs=test_data.outputs,
                                                       run_type='foo_run_type')

        assert analysis_process.get('process_core').get('process_id') == '12345abcde'
        self.verify_inputs(analysis_process.get('inputs'), test_data)

        analysis_file_schema_url = '{}/type/file/4.5.6/analysis_file'.format(test_data.schema_url)
        self.verify_outputs(analysis_process.get('outputs'), test_data.outputs, analysis_file_schema_url)

        self.verify_tasks(analysis_process.get('tasks'))

        assert analysis_process.get('schema_type') == 'process'

        schema_url = '{}/type/process/analysis/1.2.3/analysis_process'.format(test_data.schema_url)
        assert analysis_process.get('describedBy') == schema_url

        assert analysis_process.get('reference_bundle') == 'foo_ref_bundle'
        assert analysis_process.get('analysis_run_type') == 'foo_run_type'
        assert analysis_process.get('timestamp_start_utc') == '2017-09-14T19:54:11.470Z'
        assert analysis_process.get('timestamp_stop_utc') == '2017-09-14T19:54:31.871Z'
        assert analysis_process.get('input_bundles') == ['foo_input_bundle1', 'foo_input_bundle2']

    def test_create_analysis_protocol(self, test_data):
        analysis_protocol = cam.create_analysis_protocol(raw_schema_url=test_data.schema_url,
                                                         analysis_protocol_schema_version='1.2.3',
                                                         pipeline_version='foo_pipeline_version',
                                                         method='foo_method')

        assert analysis_protocol.get('protocol_core').get('protocol_id') == 'foo_pipeline_version'
        assert analysis_protocol.get('computational_method') == 'foo_method'
        assert analysis_protocol.get('schema_type') == 'protocol'

    def test_get_inputs(self, data_file, test_data):
        inputs_file = data_file('inputs.tsv')
        inputs = cam.get_inputs(inputs_file)
        self.verify_inputs(inputs, test_data, include_checksum=False)

    def test_get_input_urls(self, test_data):
        input_urls = cam.get_input_urls(test_data.inputs)
        assert len(input_urls) == 2
        assert 'gs://foo/path/read1.fastq.gz' in input_urls
        assert 'gs://foo/path/read2.fastq.gz' in input_urls

    def test_get_input_urls_empty_list(self):
        input_urls = cam.get_input_urls([])
        assert len(input_urls) == 0

    def test_get_input_urls_no_urls(self):
        inputs = [
            {
                'parameter_name': 'p1',
                'parameter_value': 'foo'
            }
        ]
        input_urls = cam.get_input_urls(inputs)
        assert len(input_urls) == 0

    def test_add_md5s(self, test_data):
        inputs = deepcopy(test_data.inputs)
        for i in inputs:
            i.pop('checksum', None)
        inputs_with_md5s = cam.add_md5s_to_inputs(test_data.inputs, test_data.input_url_to_md5)
        self.verify_inputs(inputs_with_md5s, test_data)
        # Verify that original dict was not modified
        assert len([i for i in inputs if 'checksum' in i]) == 0

    def test_base64_to_hex(self):
        base64_str = 'FdUxglEpKjfcvIRvkju7nA=='
        hex_str = '15d5318251292a37dcbc846f923bbb9c'
        assert cam.base64_to_hex(base64_str) == hex_str

    def test_get_analysis_process_core(self):
        analysis_workflow_id = 'good_id'
        process_description = 'good_description'

        analysis_process_core = cam.get_analysis_process_core(analysis_workflow_id,
                                                              process_description=process_description)
        expected_process_core = {
            'process_id': analysis_workflow_id,
            'process_description': process_description
        }
        assert analysis_process_core == expected_process_core

    def test_get_analysis_process_type(self):
        analysis_process_type = cam.get_analysis_process_type()
        expected_analysis_process_type = {'text': 'analysis'}
        assert analysis_process_type == expected_analysis_process_type

    def test_get_workflow_tasks(self, data_file):
        with open(data_file('metadata.json')) as f:
            metadata = json.load(f)
        tasks = cam.get_workflow_tasks(metadata)
        self.verify_tasks(tasks)

    def test_get_file_format(self):
        assert cam.get_file_format('asdf', {}) == 'unknown'
        assert cam.get_file_format('asdf.bam', {'.bam$': 'bam'}) == 'bam'
        assert cam.get_file_format('asdf.txt', {'.bam$': 'bam'}) == 'unknown'
        assert cam.get_file_format('asdf.bam', {'.bam$': 'bam', '_metrics': 'metrics'}) == 'bam'
        assert cam.get_file_format('asdf.foo_metrics', {'.bam$': 'bam', '_metrics$': 'metrics'}) == 'metrics'
        assert cam.get_file_format('asdf.zarr!expression_matrix!id!.zarray',
                                   {'.bam$': 'bam', '_metrics$': 'metrics', '.zarr!': 'zarr_format'}) == 'zarr_format'

    def test_get_outputs(self, test_data):
        schema_version = 'good_version'
        schema_url = '{}/type/file/{}/analysis_file'.format(test_data.schema_url, schema_version)
        outputs_json = cam.get_outputs(test_data.output_urls, test_data.extension_to_format,
                                       test_data.schema_url, schema_version)
        self.verify_outputs(outputs_json, test_data.outputs, schema_url, include_md5s=False)

    def test_get_analysis_protocol_core(self):
        pipeline_version = 'good_version'
        protocol_description = 'good_description'

        analysis_protocol_core = cam.get_analysis_protocol_core(pipeline_version,
                                                                protocol_description=protocol_description)
        expected_protocol_type = {
            'protocol_id': 'good_version',
            'protocol_description': 'good_description'
        }
        assert analysis_protocol_core == expected_protocol_type

    def test_get_analysis_protocol_type(self):
        analysis_protocol_type = cam.get_analysis_protocol_type()
        expected_analysis_protocol_type = {'text': 'analysis'}
        assert analysis_protocol_type == expected_analysis_protocol_type

    def verify_inputs(self, inputs, test_data, include_checksum=True):
        assert inputs[0]['parameter_name'] == test_data.inputs[0]['parameter_name']
        assert inputs[0]['parameter_value'] == test_data.inputs[0]['parameter_value']
        assert inputs[1]['parameter_name'] == test_data.inputs[1]['parameter_name']
        assert inputs[1]['parameter_value'] == test_data.inputs[1]['parameter_value']
        assert inputs[2]['parameter_name'] == test_data.inputs[2]['parameter_name']
        assert inputs[2]['parameter_value'] == test_data.inputs[2]['parameter_value']
        assert inputs[3]['parameter_name'] == test_data.inputs[3]['parameter_name']
        assert inputs[3]['parameter_value'] == test_data.inputs[3]['parameter_value']
        if include_checksum:
            assert inputs[0]['checksum'] == test_data.inputs[0]['checksum']
            assert inputs[1]['checksum'] == test_data.inputs[1]['checksum']

    def verify_outputs(self, output_json, expected_outputs, schema_url, include_md5s=True):
        assert output_json[0]['describedBy'] == schema_url
        assert output_json[0]['schema_type'] == 'file'
        assert output_json[0]['file_core']['file_format'] == expected_outputs[0]['file_core']['file_format']
        assert output_json[0]['file_core']['file_name'] == expected_outputs[0]['file_core']['file_name']
        if include_md5s:
            assert output_json[0]['file_core']['checksum'] == expected_outputs[0]['file_core']['checksum']
        assert output_json[0]['describedBy'] == schema_url
        assert output_json[1]['schema_type'] == 'file'
        assert output_json[1]['file_core']['file_format'] == expected_outputs[1]['file_core']['file_format']
        assert output_json[1]['file_core']['file_name'] == expected_outputs[1]['file_core']['file_name']
        if include_md5s:
            assert output_json[1]['file_core']['checksum'] == expected_outputs[1]['file_core']['checksum']

    def verify_tasks(self, tasks):
        assert len(tasks) == 5
        first_task = tasks[0]
        assert first_task['task_name'] == 'CollectAlignmentSummaryMetrics'
        assert first_task['log_out'].split('/')[-1] == 'CollectAlignmentSummaryMetrics-stdout.log'
        assert first_task['log_err'].split('/')[-1] == 'CollectAlignmentSummaryMetrics-stderr.log'
        assert first_task['start_time'] == '2017-09-14T19:54:22.691Z'
        assert first_task['stop_time'] == '2017-09-14T19:54:31.473Z'
        assert first_task['memory'] == '10 GB'
        assert first_task['zone'] == 'us-central1-b'
        assert first_task['cpus'] == 1
        assert first_task['disk_size'] == 'local-disk 10 HDD'
        assert first_task['docker_image'] == 'humancellatlas/picard:2.10.10'
