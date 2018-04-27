import unittest
import os
import json
import pipeline_tools.create_analysis_json as caj


class TestCreateAnalysisJson(unittest.TestCase):

    def test_create_analysis(self):
        js = caj.create_analysis(
            analysis_id='12345abcde',
            metadata_file=self.data_file('metadata.json'),
            input_bundles_string='foo_input_bundle1,foo_input_bundle2',
            reference_bundle='foo_ref_bundle',
            run_type='foo_run_type',
            method='foo_method',
            schema_version='v1.2.3',
            inputs_file=self.data_file('inputs.tsv'),
            outputs_file=self.data_file('outputs.txt'),
            format_map=self.data_file('format_map.json')
        )
        self.assertEqual(js.get('process_core').get('process_id'), '12345abcde')
        self.verify_inputs(js.get('inputs'))
        schema_url = 'https://schema.humancellatlas.org/type/file/v1.2.3/analysis_file'
        self.verify_outputs(js.get('outputs'), schema_url)
        self.verify_tasks(js.get('tasks'))
        self.assertEqual(js.get('schema_type'), 'process')
        schema_url = 'https://schema.humancellatlas.org/type/process/analysis/v1.2.3/analysis_process' 
        self.assertEqual(js.get('describedBy'), schema_url)
        self.assertEqual(js.get('computational_method'), 'foo_method')
        self.assertEqual(js.get('reference_bundle'), 'foo_ref_bundle')
        self.assertEqual(js.get('analysis_run_type'), 'foo_run_type')
        self.assertEqual(js.get('timestamp_start_utc'), '2017-09-14T19:54:11.470Z')
        self.assertEqual(js.get('timestamp_stop_utc'), '2017-09-14T19:54:31.871Z')
        self.assertEqual(js.get('input_bundles'), ['foo_input_bundle1', 'foo_input_bundle2'])

    def test_create_inputs(self):
        inputs_file = self.data_file('inputs.tsv')
        inputs = caj.create_inputs(inputs_file)
        self.verify_inputs(inputs)

    def test_create_outputs(self):
        outputs_file = self.data_file('outputs.txt') 
        format_map_file = self.data_file('format_map.json')
        schema_version = 'good_version'
        schema_url = 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(schema_version)
        outputs = caj.create_outputs(outputs_file, format_map_file, schema_version)
        self.verify_outputs(outputs, schema_url)

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

    def test_create_process_core(self):
        analysis_id = '12345abcde'
        schema_version = 'good_version'
        schema_url = 'https://schema.humancellatlas.org/core/process/{}/process_core'.format(schema_version)

        process_core = caj.create_process_core(analysis_id, schema_version)
        expected_core = {
            'process_id': analysis_id,
            'describedBy': schema_url,
            'schema_version': schema_version
        }
        self.assertEqual(process_core, expected_core)

    def test_create_process_type(self):
        schema_version = 'good_version'
        process_type = caj.create_process_type(schema_version)
        expected_process_type = {
            'text': 'analysis',
            'describedBy': 'https://schema.humancellatlas.org/module/ontology/{}/process_type_ontology'.format(schema_version)
        }
        self.assertEqual(process_type, expected_process_type)

    def verify_inputs(self, inputs):
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

    def verify_outputs(self, outputs, schema_url):
        self.assertEqual(outputs[0]['describedBy'], schema_url)
        self.assertEqual(outputs[0]['schema_type'], 'file')
        self.assertEqual(outputs[0]['file_core']['file_format'], 'bam')
        self.assertEqual(outputs[0]['file_core']['file_name'], 'Aligned.sortedByCoord.out.bam')
        self.assertEqual(outputs[0]['describedBy'], schema_url)
        self.assertEqual(outputs[1]['schema_type'], 'file')
        self.assertEqual(outputs[1]['file_core']['file_format'], 'metrics')
        self.assertEqual(outputs[1]['file_core']['file_name'], 'GSM1957573_rna_metrics')

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
