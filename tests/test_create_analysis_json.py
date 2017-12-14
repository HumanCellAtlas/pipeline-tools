import unittest
import os
import json
import requests_mock
from requests.exceptions import HTTPError

import pipeline_tools.create_analysis_json as analysis_json

class TestCreateAnalysisJson(unittest.TestCase):

    def test_create_inputs(self):
        inputs_file = self.data_file('inputs.tsv') 
        inputs = analysis_json.create_inputs(inputs_file)
        self.assertEqual(inputs[0]['name'], 'fastq_read1')
        self.assertEqual(inputs[0]['value'], 'gs://broad-dsde-mint-dev-teststorage/path/read1.fastq.gz')
        self.assertEqual(inputs[0]['checksum'], 'd0f7d08f1980f7980f')
        self.assertEqual(inputs[1]['name'], 'fastq_read2')
        self.assertEqual(inputs[1]['value'], 'gs://broad-dsde-mint-dev-teststorage/path/read2.fastq.gz')
        self.assertEqual(inputs[1]['checksum'], 'd0f7d08f1980f7980f')
        self.assertEqual(inputs[2]['name'], 'output_prefix')
        self.assertEqual(inputs[2]['value'], 'GSM1957573')
        self.assertEqual(inputs[3]['name'], 'test_int')
        self.assertEqual(inputs[3]['value'], '123')

    def test_create_outputs(self):
        outputs_file = self.data_file('outputs.txt') 
        format_map_file = self.data_file('format_map.json') 
        outputs = analysis_json.create_outputs(outputs_file, format_map_file)
        output_lines = []
        with open(outputs_file) as f:
            for line in f:
                output_lines.append(line.strip())
        self.assertEqual(outputs[0]['file_path'], output_lines[0])
        self.assertEqual(outputs[0]['format'], 'bam')
        self.assertEqual(outputs[0]['name'], 'Aligned.sortedByCoord.out.bam')
        self.assertEqual(outputs[1]['file_path'], output_lines[1])
        self.assertEqual(outputs[1]['format'], 'metrics')
        self.assertEqual(outputs[1]['name'], 'GSM1957573_rna_metrics')

    def test_get_input_bundles(self):
        bundles = analysis_json.get_input_bundles('foo,bar,baz')
        self.assertEqual(bundles, ['foo', 'bar', 'baz'])

    def test_get_start_end(self):
        with open(self.data_file('metadata.json')) as f:
            metadata = json.load(f)
            start, end = analysis_json.get_start_end(metadata)
            self.assertEqual(start, '2017-09-14T19:54:11.470Z')
            self.assertEqual(end, '2017-09-14T19:54:31.871Z')

    def test_get_tasks(self):
        with open(self.data_file('metadata.json')) as f:
            metadata = json.load(f)
            tasks = analysis_json.get_tasks(metadata)
            self.assertEqual(len(tasks), 5)
            first_task = tasks[0]
            self.assertEqual(first_task['name'], 'CollectAlignmentSummaryMetrics')
            self.assertEqual(first_task['log_out'].split('/')[-1], 'CollectAlignmentSummaryMetrics-stdout.log')
            self.assertEqual(first_task['log_err'].split('/')[-1], 'CollectAlignmentSummaryMetrics-stderr.log')
            self.assertEqual(first_task['start_time'], '2017-09-14T19:54:22.691Z')
            self.assertEqual(first_task['stop_time'], '2017-09-14T19:54:31.473Z')
            self.assertEqual(first_task['memory'], '10 GB')
            self.assertEqual(first_task['zone'], 'us-central1-b')
            self.assertEqual(first_task['cpus'], 1)
            self.assertEqual(first_task['disk_size'], 'local-disk 10 HDD')
            self.assertEqual(first_task['docker_image'], 'humancellatlas/picard:2.10.10')

    def test_get_format(self):
        self.assertEqual(analysis_json.get_format('asdf', {}), 'unknown')
        self.assertEqual(analysis_json.get_format('asdf.bam', {'.bam': 'bam'}), 'bam')
        self.assertEqual(analysis_json.get_format('asdf.txt', {'.bam': 'bam'}), 'unknown')
        self.assertEqual(analysis_json.get_format('asdf.bam', {'.bam': 'bam', '_metrics': 'metrics'}), 'bam')
        self.assertEqual(analysis_json.get_format('asdf.foo_metrics', {'.bam': 'bam', '_metrics': 'metrics'}), 'metrics')

    @requests_mock.mock()
    def test_create_core_success(self, mock):
        type = 'analysis'
        schema_version = 'good_version'
        schema_url = 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/{0}/json_schema/analysis.json'.format(schema_version)

        mock.head(schema_url, status_code=200)
        core = analysis_json.create_core(type=type, schema_version=schema_version)
        expected_core = {
            'type': type,
            'schema_url': schema_url,
            'schema_version': schema_version
        }
        self.assertEquals(core, expected_core)

    @requests_mock.mock()
    def test_create_core_failure(self, mock):
        type = 'analysis'
        schema_version = 'bad_version'
        schema_url = 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/{0}/json_schema/analysis.json'.format(schema_version)

        mock.head(schema_url, status_code=404)

        try:
            core = analysis_json.create_core(type=type, schema_version=schema_version)
        except HTTPError:
            pass

    @requests_mock.mock()
    def test_create_analysis_bundle(self, mock):
        fake_analysis_content = {
            'metadata_schema': 'good_version'
        }
        schema_version = 'good_version'
        schema_url = 'https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/{0}/json_schema/analysis_bundle.json'.format(schema_version)

        mock.head(schema_url, status_code=200)

        analysis_bundle = analysis_json.create_analysis_bundle(fake_analysis_content)
        self.assertEquals(analysis_bundle.get('content'), fake_analysis_content)
        self.assertEquals(analysis_bundle.get('core').get('schema_version'), 'good_version')

    def data_file(self, file_name):
        return os.path.split(__file__)[0] + '/data/'  + file_name


if __name__ == '__main__':
    unittest.main()
