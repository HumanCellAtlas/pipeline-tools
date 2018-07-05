#!/usr/bin/env python

import argparse
import base64
import codecs
from copy import deepcopy
from csv import DictReader
from google.cloud import storage
import json


def create_analysis(analysis_id, metadata_file, input_bundles_string, reference_bundle,
        run_type, method, schema_version, analysis_file_version, inputs, output_url_to_md5,
        extension_to_format):
    """Creates analysis json for submission.

    Args:
        analysis_id (str): the workflow id of the analysis
        metadata_file (str): path to file containing metadata json for the workflow
        input_bundles_string (str): a comma-separated list of input bundle uuids
        reference_bundle (str): the uuid of the reference bundle
        run_type (str): indicates whether this analysis was actually run or is just copying previous results
        method (str): the name of the workflow
        schema_version (str): the version of the metadata schema that the analysis json conforms to
        inputs_file (str): path to file containing metadata about workflow inputs
        output_url_to_md5 (dict): dict mappiung workflow output urls to corresponding md5 hashes
        extension_to_format (dict): dict mapping file extensions to file formats

    Returns:
        analysis (dict): A dict representing the analysis json file to be submitted
    """
    print('Creating analysis.json for {}'.format(analysis_id))

    with open(metadata_file) as f:
        metadata = json.load(f)
        tasks = get_tasks(metadata)

    input_bundles = input_bundles_string.split(',')

    outputs = create_outputs(output_url_to_md5, extension_to_format, analysis_file_version)

    schema_url = 'https://schema.humancellatlas.org/type/protocol/analysis/{}/analysis_protocol'.format(schema_version)

    analysis = {
        'analysis_run_type': run_type,
        'reference_bundle': reference_bundle,
        'computational_method': method,
        'input_bundles': input_bundles,
        'timestamp_start_utc': metadata.get('start'),
        'timestamp_stop_utc': metadata.get('end'),
        'tasks': tasks,
        'inputs': inputs,
        'outputs': outputs,
        'protocol_core': create_protocol_core(analysis_id),
        'protocol_type': create_protocol_type(),
        'schema_type': 'protocol',
        'describedBy': schema_url
    }

    # Add logging
    print('The content of analysis.json: ')
    print(analysis)

    return analysis


def get_inputs(inputs_file):
    """Reads input parameter names and values from tsv file

    Args:
        inputs_file (str): Path to tsv file of parameter names and values

    Returns:
        inputs (list of dict): A list of dicts, where each dict gives the name and value of a single parameter
    """
    inputs = []
    with open(inputs_file) as f:
        f.readline() # skip header
        reader = DictReader(f, lineterminator='\n', delimiter='\t', fieldnames=['parameter_name', 'parameter_value'])
        for line in reader:
            inputs.append(line)
    return inputs


def get_input_urls(inputs):
    """Returns just the gs urls from the given list of inputs

    Args:
        inputs (list of dict): A list of input parameter dicts

    Returns:
        (list of str): list of gs urls
    """
    return [i['parameter_value'] for i in inputs if i['parameter_value'].startswith('gs://')]


def add_md5s(inputs, input_url_to_md5):
    """Adds md5 hash for each file in the given list.

    Queries GCS for the md5 of each parameter whose value starts with 'gs://'.

    Args:
        inputs (list of dict): List of dicts containing input parameter names and values
        input_url_to_md5 (dict): dict of gs urls to corresponding md5 hashes

    Returns:
        inputs (list of dict): A list of dicts, where each dict contains the name, value, and md5 hash (for gs urls)
    """
    inputs_with_md5 = []
    for i in inputs:
        value = i['parameter_value'] 
        if value.startswith('gs://'):
            input_with_md5 = deepcopy(i)
            input_with_md5['checksum'] = input_url_to_md5[value]
            inputs_with_md5.append(input_with_md5)
        else:
            inputs_with_md5.append(deepcopy(i))
    return inputs_with_md5


def get_md5s(output_urls, gcs_client):
    """Gets md5s

    Args:
        output_urls (list of str): the gs urls to get md5s for
        gcs_client (google.cloud.storage.Client): the client to use to get the md5s

    Returns:
        url_to_md5 (dict): dict of gs urls to md5 hashes
    """
    url_to_md5 = {}
    for url in output_urls:
        md5 = get_md5(url, gcs_client)
        url_to_md5[url] = md5
    return url_to_md5


def get_md5(gs_url, gcs_client):
    """Gets md5 hash for the given gs url

    Args:
        gs_url (str): the gs url
        gcs_client (google.cloud.storage.Client): the client to use to get the md5

    Returns:
        hex_str (str): the md5 hash, hexadecimal-encoded
    """
    _, _, bucket_str, path_str = gs_url.split('/', 3)
    bucket = gcs_client.bucket(bucket_str)
    blob = bucket.get_blob(path_str)
    hex_str = base64_to_hex(blob.md5_hash)
    return hex_str


def base64_to_hex(base64_str):
    """Converts a base64 string to the equivalent hexadecimal string

    Args:
        base64_str (str): A string representing base64-encoded data

    Returns:
        hex_str (str): A string giving the hexadecimal-encoded representation of the given data
    """
    base64_bytes = base64.b64decode(base64_str.strip('\n"\''))
    hex_bytes = codecs.encode(base64_bytes, 'hex')
    hex_str = hex_bytes.decode('utf-8')
    return hex_str


def create_outputs(output_url_to_md5, extension_to_format, analysis_file_version):
    """Creates outputs metadata array for analysis json

    Args:
        output_url_to_md5 (dict): dict of output gs urls to corresponding md5 hashs
        extension_to_format (dict): dict of file extensions to corresponding file formats
        analysis_file_version (str): the version of the metadata schema that the output file json should conform to

    Returns:
        outputs (list): Array of dicts representing outputs metadata in the format required for the analysis json file.
    """

    outputs = []
    for output_url, md5_hash in sorted(output_url_to_md5.items()):
        d = {
          'describedBy': 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(analysis_file_version),
          'schema_type': 'file',
          'file_core': {
            'file_name': output_url.split('/')[-1],
            'file_format': get_format(output_url, extension_to_format),
            'checksum': md5_hash
          }
        }
        outputs.append(d)

    # Add logging
    print('The content of outputs: ')
    print(outputs)

    return outputs


def get_format(path, extension_to_format):
    """Returns the file type of the file at the given path

    Args:
        path (str): the path to the file
        extension_to_format (dict): dict mapping file extensions to file types

    Returns:
        str: A string representing the format of the file
    """
    for ext in extension_to_format:
        if path.endswith(ext):
            format = extension_to_format[ext]
            print(format)
            return format
    print('Warning: no known format in the format_map matches file {}'.format(path))
    return 'unknown'


def get_tasks(metadata):
    """Creates array of task metadata for analysis json

    Args:
        metadata (dict): the workflow metadata

    Returns:
        sorted_output_tasks (list): Sorted array of dicts representing task metadata in the format required for
            the analysis json.
    """
    calls = metadata['calls']

    output_tasks = []
    for long_task_name in calls:
        task_name = long_task_name.split('.')[-1]
        task = calls[long_task_name][0]

        if task.get('subWorkflowMetadata'):
            output_tasks.extend(get_tasks(task['subWorkflowMetadata']))
        else:
            runtime = task['runtimeAttributes']
            out_task = {
                'task_name': task_name,
                'cpus': int(runtime['cpu']),
                'memory': runtime['memory'],
                'disk_size': runtime['disks'],
                'docker_image': runtime['docker'],
                'zone': runtime['zones'],
                'start_time': task['start'],
                'stop_time': task['end'],
                'log_out': task['stdout'],
                'log_err': task['stderr']
            }
            output_tasks.append(out_task)
    sorted_output_tasks = sorted(output_tasks, key=lambda k: k['task_name'])
    return sorted_output_tasks


def create_protocol_core(analysis_id):
    """Creates process_core entry for analysis json

    Args:
        analysis_id (str): the workflow id

    Returns:
        dict: Dict containing process_core metadata required for analysis json
    """
    return {
        'protocol_id': analysis_id,
    }


def create_protocol_type():
    """Creates process_type metadata for analysis json

    Returns:
        dict: Dict containing process_type metadata in the forma required for the analysis json
    """
    return {
        'text': 'analysis'
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_id', required=True, help='Cromwell workflow id')
    parser.add_argument('--metadata_json', required=True, help='JSON obtained from calling Cromwell metadata endpoint for this workflow id')
    parser.add_argument('--input_bundles', required=True, help='The DSS bundles used as inputs for the workflow')
    parser.add_argument('--reference_bundle', required=True, help='To refer to the DSS resource bundle used for this workflow, once such things exist')
    parser.add_argument('--run_type', required=True, help='Should always be "run" for now, may be "copy-forward" in some cases in future')
    parser.add_argument('--method', required=True, help='Supposed to be method store url, for now can be url for wdl in skylab')
    parser.add_argument('--schema_version', required=True, help='The metadata schema version that this analysis.json conforms to')
    parser.add_argument('--analysis_file_version', required=True, help='The metadata schema version that the output files conform to')
    parser.add_argument('--inputs_file', required=True, help='Path to tsv containing info about inputs')
    parser.add_argument('--outputs_file', required=True, help='Path to json file containing info about outputs')
    parser.add_argument('--format_map', required=True, help='JSON file providing map of file extensions to formats')
    args = parser.parse_args()

    with open(args.format_map) as f:
        extension_to_format = json.load(f)
    with open(args.outputs_file) as f:
        output_urls = f.read().splitlines()
    client = storage.Client()
    output_url_to_md5 = get_md5s(output_urls, client)

    inputs = get_inputs(args.inputs_file)
    input_urls = get_input_urls(inputs)
    input_url_to_md5 = get_md5s(input_urls, client)
    inputs = add_md5s(inputs, input_url_to_md5)
    analysis = create_analysis(args.analysis_id, args.metadata_json, args.input_bundles,
        args.reference_bundle, args.run_type, args.method, args.schema_version, args.analysis_file_version,
        inputs, output_url_to_md5, extension_to_format)

    with open('analysis.json', 'w') as f:
        json.dump(analysis, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
