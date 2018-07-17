from copy import deepcopy
import unittest
import os
import json
from google.cloud import storage
import pipeline_tools.create_analysis_json as caj


class TestCreateAnalysisJson(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.output_url_to_md5 = {
            'gs://foo/bar/Aligned.sortedByCoord.out.bam': '0123456789abcdef0123456789abcdef',
            'gs://foo/bar/GSM1957573_rna_metrics': 'abcdef0123456789abcdef0123456789'
        }
        self.extension_to_format = {
            '.bam': 'bam',
            '_metrics': 'metrics'
        }
        self.output_dicts = [
            {
                'name': 'Aligned.sortedByCoord.out.bam',
                'format': 'bam',
                'checksum': '0123456789abcdef0123456789abcdef'
            },
            {
                'name': 'GSM1957573_rna_metrics',
                'format': 'metrics',
                'checksum': 'abcdef0123456789abcdef0123456789'
            }
        ]
        self.inputs = [
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
        self.input_url_to_md5 = {
            'gs://foo/path/read1.fastq.gz': '0123456789abcdef0123456789abcdef',
            'gs://foo/path/read2.fastq.gz': 'abcdef0123456789abcdef0123456789'
        }

    def test_create_analysis(self):

        js = caj.create_analysis(
            analysis_id='12345abcde',
            metadata_file=self.data_file('metadata.json'),
            input_bundles_string='foo_input_bundle1,foo_input_bundle2',
            reference_bundle='foo_ref_bundle',
            run_type='foo_run_type',
            method='foo_method',
            schema_version='1.2.3',
            analysis_file_version='4.5.6',
            inputs=self.inputs,
            output_url_to_md5=self.output_url_to_md5,
            extension_to_format=self.extension_to_format
        )
        self.assertEqual(js.get('protocol_core').get('protocol_id'), '12345abcde')
        self.verify_inputs(js.get('inputs'))
        file_schema_url = 'https://schema.humancellatlas.org/type/file/4.5.6/analysis_file'
        self.verify_outputs(js.get('outputs'), self.output_dicts, file_schema_url)
        self.verify_tasks(js.get('tasks'))
        self.assertEqual(js.get('schema_type'), 'protocol')
        schema_url = 'https://schema.humancellatlas.org/type/protocol/analysis/1.2.3/analysis_protocol'
        self.assertEqual(js.get('describedBy'), schema_url)
        self.assertEqual(js.get('computational_method'), 'foo_method')
        self.assertEqual(js.get('reference_bundle'), 'foo_ref_bundle')
        self.assertEqual(js.get('analysis_run_type'), 'foo_run_type')
        self.assertEqual(js.get('timestamp_start_utc'), '2017-09-14T19:54:11.470Z')
        self.assertEqual(js.get('timestamp_stop_utc'), '2017-09-14T19:54:31.871Z')
        self.assertEqual(js.get('input_bundles'), ['foo_input_bundle1', 'foo_input_bundle2'])

    def test_get_inputs(self):
        inputs_file = self.data_file('inputs.tsv')
        inputs = caj.get_inputs(inputs_file)
        self.verify_inputs(inputs, include_checksum=False)

    def test_create_outputs(self):
        schema_version = 'good_version'
        schema_url = 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(schema_version)
        outputs_json = caj.create_outputs(self.output_url_to_md5, self.extension_to_format, schema_version)
        self.verify_outputs(outputs_json, self.output_dicts, schema_url)

    def test_get_tasks(self):
        with open(self.data_file('metadata.json')) as f:
            metadata = json.load(f)
        tasks = caj.get_tasks(metadata)
        self.verify_tasks(tasks)

    def test_get_format(self):
        self.assertEqual(caj.get_format('asdf', {}), 'unknown')
        self.assertEqual(caj.get_format('asdf.bam', {'.bam': 'bam'}), 'bam')
        self.assertEqual(caj.get_format('asdf.txt', {'.bam': 'bam'}), 'unknown')
        self.assertEqual(caj.get_format('asdf.bam', {'.bam': 'bam', '_metrics': 'metrics'}), 'bam')
        self.assertEqual(caj.get_format('asdf.foo_metrics', {'.bam': 'bam', '_metrics': 'metrics'}), 'metrics')

    def test_create_protocol_core(self):
        analysis_id = '12345abcde'

        protocol_core = caj.create_protocol_core(analysis_id)
        expected_core = {
            'protocol_id': analysis_id
        }
        self.assertEqual(protocol_core, expected_core)

    def test_create_protocol_type(self):
        schema_version = 'good_version'
        protocol_type = caj.create_protocol_type()
        expected_protocol_type = {
            'text': 'analysis',
        }
        self.assertEqual(protocol_type, expected_protocol_type)

    def test_base64_to_hex(self):
        base64_str = 'FdUxglEpKjfcvIRvkju7nA=='
        hex_str = '15d5318251292a37dcbc846f923bbb9c'
        self.assertEqual(caj.base64_to_hex(base64_str), hex_str)

    def test_get_input_urls(self):
        input_urls = caj.get_input_urls(self.inputs)
        self.assertEqual(len(input_urls), 2)
        self.assertIn('gs://foo/path/read1.fastq.gz', input_urls)
        self.assertIn('gs://foo/path/read2.fastq.gz', input_urls)

    def test_get_input_urls_empty_list(self):
        input_urls = caj.get_input_urls([])
        self.assertEqual(len(input_urls), 0)

    def test_get_input_urls_no_urls(self):
        inputs = [
            {
                'parameter_name': 'p1',
                'parameter_value': 'foo'
            }
        ]
        input_urls = caj.get_input_urls(inputs)
        self.assertEqual(len(input_urls), 0)

    def test_add_md5s(self):
        inputs = deepcopy(self.inputs)
        for i in inputs:
            i.pop('checksum', None)
        inputs_with_md5s = caj.add_md5s(self.inputs, self.input_url_to_md5)
        self.verify_inputs(inputs_with_md5s)
        # Verify that original dict was not modified
        self.assertEqual(len([i for i in inputs if 'checksum' in i]), 0)

    def verify_inputs(self, inputs, include_checksum=True):
        self.assertEqual(inputs[0]['parameter_name'], self.inputs[0]['parameter_name'])
        self.assertEqual(inputs[0]['parameter_value'], self.inputs[0]['parameter_value'])
        self.assertEqual(inputs[1]['parameter_name'], self.inputs[1]['parameter_name'])
        self.assertEqual(inputs[1]['parameter_value'], self.inputs[1]['parameter_value'])
        self.assertEqual(inputs[2]['parameter_name'], self.inputs[2]['parameter_name'])
        self.assertEqual(inputs[2]['parameter_value'], self.inputs[2]['parameter_value'])
        self.assertEqual(inputs[3]['parameter_name'], self.inputs[3]['parameter_name'])
        self.assertEqual(inputs[3]['parameter_value'], self.inputs[3]['parameter_value'])
        if include_checksum:
            self.assertEqual(inputs[0]['checksum'], self.inputs[0]['checksum'])
            self.assertEqual(inputs[1]['checksum'], self.inputs[1]['checksum'])

    def verify_outputs(self, output_json, output_dicts, schema_url):
        self.assertEqual(output_json[0]['describedBy'], schema_url)
        self.assertEqual(output_json[0]['schema_type'], 'file')
        self.assertEqual(output_json[0]['file_core']['file_format'], output_dicts[0]['format'])
        self.assertEqual(output_json[0]['file_core']['file_name'], output_dicts[0]['name'])
        self.assertEqual(output_json[0]['file_core']['checksum'], output_dicts[0]['checksum'])
        self.assertEqual(output_json[0]['describedBy'], schema_url)
        self.assertEqual(output_json[1]['schema_type'], 'file')
        self.assertEqual(output_json[1]['file_core']['file_format'], output_dicts[1]['format'])
        self.assertEqual(output_json[1]['file_core']['file_name'], output_dicts[1]['name'])
        self.assertEqual(output_json[1]['file_core']['checksum'], output_dicts[1]['checksum'])

    def verify_tasks(self, tasks):
        self.assertEqual(len(tasks), 5)
        first_task = tasks[0]
        self.assertEqual(first_task['task_name'], 'CollectAlignmentSummaryMetrics')
        self.assertEqual(first_task['log_out'].split('/')[-1], 'CollectAlignmentSummaryMetrics-stdout.log')
        self.assertEqual(first_task['log_err'].split('/')[-1], 'CollectAlignmentSummaryMetrics-stderr.log')
        self.assertEqual(first_task['start_time'], '2017-09-14T19:54:22.691Z')
        self.assertEqual(first_task['stop_time'], '2017-09-14T19:54:31.473Z')
        self.assertEqual(first_task['memory'], '10 GB')
        self.assertEqual(first_task['zone'], 'us-central1-b')
        self.assertEqual(first_task['cpus'], 1)
        self.assertEqual(first_task['disk_size'], 'local-disk 10 HDD')
        self.assertEqual(first_task['docker_image'], 'humancellatlas/picard:2.10.10')

    @staticmethod
    def data_file(file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name


if __name__ == '__main__':
    unittest.main()
