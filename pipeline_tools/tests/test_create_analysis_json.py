import unittest
import os
import json
import pipeline_tools.create_analysis_json as analysis_json


class TestCreateAnalysisJson(unittest.TestCase):

    def test_create_inputs(self):
        inputs_file = self.data_file('inputs.tsv')
        inputs = analysis_json.create_inputs(inputs_file)
        self.assertEqual(inputs[0]['parameter_name'], 'fastq_read1')
        self.assertEqual(inputs[0]['parameter_value'], 'gs://broad-dsde-mint-dev-teststorage/path/read1.fastq.gz')
        self.assertEqual(inputs[0]['checksum'], 'd0f7d08f1980f7980f')
        self.assertEqual(inputs[1]['parameter_name'], 'fastq_read2')
        self.assertEqual(inputs[1]['parameter_value'], 'gs://broad-dsde-mint-dev-teststorage/path/read2.fastq.gz')
        self.assertEqual(inputs[1]['checksum'], 'd0f7d08f1980f7980f')
        self.assertEqual(inputs[2]['parameter_name'], 'output_prefix')
        self.assertEqual(inputs[2]['parameter_value'], 'GSM1957573')
        self.assertEqual(inputs[3]['parameter_name'], 'test_int')
        self.assertEqual(inputs[3]['parameter_value'], '123')

    def test_create_outputs(self):
        outputs_file = self.data_file('outputs.txt') 
        format_map_file = self.data_file('format_map.json')
        schema_version = 'good_version'
        schema_url = 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(schema_version)
        outputs = analysis_json.create_outputs(outputs_file, format_map_file, schema_version)
        output_lines = []
        with open(outputs_file) as f:
            for line in f:
                output_lines.append(line.strip())
        self.assertEqual(outputs[0]['describedBy'], schema_url)
        self.assertEqual(outputs[0]['schema_type'], 'file')
        self.assertEqual(outputs[0]['file_core']['file_format'], 'bam')
        self.assertEqual(outputs[0]['file_core']['file_name'], 'Aligned.sortedByCoord.out.bam')
        self.assertEqual(outputs[0]['describedBy'], schema_url)
        self.assertEqual(outputs[1]['schema_type'], 'file')
        self.assertEqual(outputs[1]['file_core']['file_format'], 'metrics')
        self.assertEqual(outputs[1]['file_core']['file_name'], 'GSM1957573_rna_metrics')

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

    def test_get_format(self):
        self.assertEqual(analysis_json.get_format('asdf', {}), 'unknown')
        self.assertEqual(analysis_json.get_format('asdf.bam', {'.bam': 'bam'}), 'bam')
        self.assertEqual(analysis_json.get_format('asdf.txt', {'.bam': 'bam'}), 'unknown')
        self.assertEqual(analysis_json.get_format('asdf.bam', {'.bam': 'bam', '_metrics': 'metrics'}), 'bam')
        self.assertEqual(analysis_json.get_format('asdf.foo_metrics', {'.bam': 'bam', '_metrics': 'metrics'}), 'metrics')

    def test_create_process_core(self):
        analysis_id = '12345abcde'
        schema_version = 'good_version'
        schema_url = 'https://schema.humancellatlas.org/core/process/{}/process_core'.format(schema_version)

        process_core = analysis_json.create_process_core(analysis_id, schema_version)
        expected_core = {
            'process_id': analysis_id,
            'describedBy': schema_url,
            'schema_version': schema_version
        }
        self.assertEqual(process_core, expected_core)

    def test_create_process_type(self):
        schema_version = 'good_version'
        process_type = analysis_json.create_process_type(schema_version)
        expected_process_type = {
            'text': 'analysis',
            'describedBy': 'https://schema.humancellatlas.org/module/ontology/{}/process_type_ontology'.format(schema_version)
        }
        self.assertEqual(process_type, expected_process_type)

    @staticmethod
    def data_file(file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name


if __name__ == '__main__':
    unittest.main()
