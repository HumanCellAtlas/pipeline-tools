#!/usr/bin/env python
import argparse
import base64
import codecs
import json
from copy import deepcopy
from csv import DictReader
from google.cloud import storage
import re
import uuid
import arrow
from pipeline_tools.shared.submission.format_map import EXTENSION_TO_FORMAT


def create_analysis_process(
    raw_schema_url,
    metadata_file,
    analysis_process_schema_version,
    analysis_id,
    inputs,
    run_type,
    version,
):
    """Collect and create the information about the analysis process for submission to Ingest service.

    Based the design of this HCA metadata type, analysis_process will vary between each analysis run, even if they share
    the same version of pipeline. Most of the content of this process will be collected from the Cromwell workflow
    metadata. This function strictly follows the metadata schema defined at:
    https://github.com/HumanCellAtlas/metadata-schema/blob/integration/json_schema/type/process/
    analysis/analysis_process.json

    TODO: Implement the dataclass in "https://github.com/HumanCellAtlas/metadata-api/blob/1b7192cecbef43b5befecc4153bf
    2e2f4db5bb16/src/humancellatlas/data/metadata/__init__.py#L255" so we can use the `metadata-api` directly to create
    the analysis_process metadata file.

    Args:
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        metadata_file (str): Path to file containing metadata json for the workflow.
        analysis_process_schema_version (str): Version of the metadata schema that the analysis_process conforms to.
        analysis_id (str): UUID of the analysis workflow.
        inputs (List[dict]): A list of dicts, where each dict gives the name and value of a single parameter.
        run_type (str): Indicator of whether the analysis actually ran or was just copied forward as an optimization.
                        Should be either "run" or "copy-forward".

    Returns:
        analysis_process (dict): A dict representing the analysis_process json file to be submitted.
    """
    SCHEMA_TYPE = 'process'

    workflow_metadata = get_workflow_metadata(metadata_file)
    workflow_tasks = get_workflow_tasks(workflow_metadata)

    analysis_process = {
        'describedBy': get_analysis_described_by(
            schema_url=raw_schema_url,
            schema_type=SCHEMA_TYPE,
            schema_version=analysis_process_schema_version,
        ),
        'schema_type': SCHEMA_TYPE,
        'process_core': get_analysis_process_core(analysis_workflow_id=analysis_id),
        'provenance': {'document_id': analysis_id, 'submission_date': version},
        'type': get_analysis_process_type(),
        'timestamp_start_utc': format_timestamp(workflow_metadata.get('start')),
        'timestamp_stop_utc': format_timestamp(workflow_metadata.get('end')),
        'tasks': workflow_tasks,
        'inputs': inputs,
        'analysis_run_type': run_type,
        'reference_files': [
            '00000000-0000-0000-0000-000000000000'
        ],  # TODO: Make this actually do something
    }
    return analysis_process


def create_analysis_protocol(
    raw_schema_url, analysis_protocol_schema_version, pipeline_version, method, version
):
    """Collect and create the information about the analysis protocol for submission to Ingest service.

    Based the design of this HCA metadata type, one analysis_protocol will be shared by every analysis run for a given
    pipeline, although the same content will be submitted to the protocols endpoint for each run. Besides, any changes
    to the pipeline version will change the content of the standard analysis_protocol. This function strictly follows
    the metadata schema defined at:
    https://github.com/HumanCellAtlas/metadata-schema/blob/integration/json_schema/type/process/
    analysis/analysis_process.json

    TODO: Implement the dataclass in "https://github.com/HumanCellAtlas/metadata-api/blob/1b7192cecbef43b5befecc4153bf
    2e2f4db5bb16/src/humancellatlas/data/metadata/__init__.py#L339" so we can use the `metadata-api` directly to create
    the analysis_protocol metadata file.

    Args:
        pipeline_version (str): The version of the pipeline, usually provided by the label of the adapter workflow
                                around the analysis workflow.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        analysis_protocol_schema_version (str): Version of the metadata schema that the analysis_protocol conforms to.
        method (str): The name of the analysis workflow, e.g. "SmartSeq2SingleCell"

    Returns:
        analysis_protocol (dict): A dict representing the analysis_protocol json file to be submitted.
    """
    SCHEMA_TYPE = 'protocol'
    NAMESPACE = uuid.UUID('c6591d1d-27bc-4c94-bd54-1b51f8a2456c')

    analysis_protocol = {
        'describedBy': get_analysis_described_by(
            schema_url=raw_schema_url,
            schema_type=SCHEMA_TYPE,
            schema_version=analysis_protocol_schema_version,
        ),
        'schema_type': SCHEMA_TYPE,
        'protocol_core': get_analysis_protocol_core(pipeline_version=pipeline_version),
        'computational_method': method,
        'type': get_analysis_protocol_type(),
    }

    string_to_hash = json.dumps(analysis_protocol, sort_keys=True)
    entity_id = str(uuid.uuid5(NAMESPACE, string_to_hash)).lower()

    analysis_protocol['provenance'] = {
        'document_id': entity_id,
        'submission_date': version,
        'update_date': version,
    }

    return analysis_protocol


def get_inputs(inputs_file):
    """Reads input parameter names and values from tsv file.

    Args:
        inputs_file (str): Path to tsv file of parameter names and values.

    Returns:
        inputs (List[dict]): A list of dicts, where each dict gives the name and value of a single parameter.
    """
    with open(inputs_file) as f:
        f.readline()  # skip header
        reader = DictReader(
            f,
            lineterminator='\n',
            delimiter='\t',
            fieldnames=['parameter_name', 'parameter_value'],
        )
        inputs = [line for line in reader]
    return inputs


def get_outputs(output_urls, extension_to_format, schema_url, analysis_file_version):
    """Creates outputs metadata array for analysis json.

    TODO: Implement the dataclass in "https://github.com/HumanCellAtlas/metadata-api/blob/1b7192cecbef43b5befecc4153bf
    2e2f4db5bb16/src/humancellatlas/data/metadata/__init__.py#L445" so we can use the `metadata-api` directly to create
    the analysis_file metadata file.

    Args:
        output_urls (List[str]): List of output gs urls
        extension_to_format (dict): dict of file extensions to corresponding file formats
        schema_url (str): URL for retrieving HCA metadata schemas
        analysis_file_version (str): the version of the metadata schema that the output file json should conform to

    Returns:
        outputs (List[dict]): Array of dicts representing outputs metadata in the format required for the analysis json
                              file
    """
    outputs = [
        {
            'describedBy': '{0}/type/file/{1}/analysis_file'.format(
                schema_url, analysis_file_version
            ),
            'schema_type': 'file',
            'file_core': {
                'file_name': output_url.split('/')[-1],
                'format': get_file_format(output_url, extension_to_format),
            },
        }
        for output_url in sorted(output_urls)
    ]
    return outputs


def get_input_urls(inputs):
    """Returns just the gs urls from the given list of inputs.

    Args:
        inputs (List[dict]): A list of input parameter dicts.

    Returns:
        (List[str]): list of gs urls
    """
    return [
        i['parameter_value'] for i in inputs if i['parameter_value'].startswith('gs://')
    ]


def add_md5s_to_inputs(inputs, input_url_to_md5):
    """Adds md5 hash for each file in the given list.

    Queries GCS for the md5 of each parameter whose value starts with 'gs://'.

    Args:
        inputs (List[dict]): List of dicts containing input parameter names and values.
        input_url_to_md5 (dict): Dict of gs urls to corresponding md5 hashes.

    Returns:
        inputs (List[dict]): A list of dicts, where each dict contains the name, value, and md5 hash (for gs urls).
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


def add_md5s_to_outputs(outputs, output_name_to_md5):
    """Adds md5 hash for each file in the given list.

    Queries GCS for the md5 of each parameter whose value starts with 'gs://'.

    Args:
        outputs (List[dict]): List of dicts containing output metadata
        output_name_to_md5 (dict): Dict of file names to corresponding md5 hashes

    Returns:
        outputs_with_md5 (List[dict]): A list of dicts, where each dict contains metadata for an output file
    """
    outputs_with_md5 = []
    for out in outputs:
        name = out['file_core']['file_name']
        output_with_md5 = deepcopy(out)
        output_with_md5['file_core']['checksum'] = output_name_to_md5[name]
        outputs_with_md5.append(output_with_md5)
    return outputs_with_md5


def get_md5s(output_urls, gcs_client):
    """Gets md5s.

    Args:
        output_urls (List[str]): The gs urls to get md5s for.
        gcs_client (google.cloud.storage.Client): The client to use to get the md5s.

    Returns:
        url_to_md5 (dict): Dict of gs urls to md5 hashes.
    """
    url_to_md5 = {url: get_md5(url, gcs_client) for url in output_urls}
    return url_to_md5


def get_md5(gs_url, gcs_client):
    """Gets md5 hash for the given gs url.

    Args:
        gs_url (str): The gs url.
        gcs_client (google.cloud.storage.Client): the client object to use to get the md5.

    Returns:
        hex_str (str): The md5 hash, hexadecimal-encoded.
    """
    _, _, bucket_str, path_str = gs_url.split('/', 3)
    bucket = gcs_client.bucket(bucket_str)
    blob = bucket.get_blob(path_str)
    hex_str = base64_to_hex(blob.md5_hash)
    return hex_str


def base64_to_hex(base64_str):
    """Converts a base64 string to the equivalent hexadecimal string.

    Args:
        base64_str (str): A string representing base64-encoded data.

    Returns:
        hex_str (str): A string giving the hexadecimal-encoded representation of the given data.
    """
    base64_bytes = base64.b64decode(base64_str.strip('\n"\''))
    hex_bytes = codecs.encode(base64_bytes, 'hex')
    hex_str = hex_bytes.decode('utf-8')
    return hex_str


def get_analysis_described_by(schema_url, schema_type, schema_version):
    """A helper function prepares the "describedBy" field in the analysis metadata, works for both process and protocol.

    Args:
        schema_url (str): URL for retrieving HCA metadata schemas.
        schema_type (str): 'process' or 'protocol', indicates the type of the schema.
        schema_version (str): The version of the metadata schema that analysis_process/analysis_protocol conforms to.

    Returns:
        schema_url_reference (str): URL for retrieving the specific HCA metadata schemas, fulfilling the
                                    "describedBy" field in the analysis metadata.
    """
    schema_url_reference = '{schema_url}/type/{schema_type}/analysis/{schema_version}/analysis_{schema_type}'.format(
        schema_url=schema_url, schema_type=schema_type, schema_version=schema_version
    )
    return schema_url_reference


def get_analysis_process_core(analysis_workflow_id, **kwargs):
    """Creates process_core metadata for analysis_process.

    This defines the core entry of the analysis process, the process_id usually varies between runs, in this case, it
    is set to be the analysis workflow UUID, which is unique within the Cromwell execution context.

    Args:
        analysis_workflow_id (str): UUID of the analysis workflow.
        **kwargs: Non-required but optional fields for analysis_process core. e.g. process_name, process_description.

    Returns:
        analysis_process_core (dict): Dict containing process_core metadata required for analysis_process.
    """
    analysis_process_core = {'process_id': analysis_workflow_id}
    for optional_key, optional_val in kwargs.items():
        analysis_process_core[optional_key] = optional_val
    return analysis_process_core


def get_analysis_process_type():
    """Creates process_type metadata for analysis_process.

    This defines the type of process, which refers to the process_type_ontology. The only required key of the dict
    should be 'text', and the value should be the name of a process type being used.

    Returns:
        analysis_process_type (dict): The process_type metadata for analysis_process.
    """
    analysis_process_type = {'text': 'analysis'}
    return analysis_process_type


def get_workflow_metadata(metadata_file):
    """Load workflow metadata from a JSON file.

    Args:
        metadata_file (str): Path to file containing metadata json for the workflow.

    Returns:
        metadata (dict): A dict consisting of Cromwell workflow metadata information.
    """
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata


def get_workflow_tasks(workflow_metadata):
    """Creates array of Cromwell workflow's task metadata for analysis_process.

    Args:
        workflow_metadata (dict): A dict representing the workflow metadata.

    Returns:
        sorted_output_tasks (list): Sorted array of dicts representing task metadata in the format required for
                                    the analysis json.
    """
    calls = workflow_metadata['calls']

    output_tasks = []
    for long_task_name in calls:
        task_name = long_task_name.split('.')[-1]
        task = calls[long_task_name][0]

        if task.get('subWorkflowMetadata'):
            output_tasks.extend(
                get_workflow_tasks(task['subWorkflowMetadata'])
            )  # recursively parse tasks
        else:
            runtime = task['runtimeAttributes']
            out_task = {
                'task_name': task_name,
                'cpus': int(runtime['cpu']),
                'memory': runtime['memory'],
                'disk_size': runtime['disks'],
                'docker_image': runtime['docker'],
                'zone': runtime['zones'],
                'start_time': format_timestamp(task['start']),
                'stop_time': format_timestamp(task['end']),
                'log_out': task['stdout'],
                'log_err': task['stderr'],
            }
            output_tasks.append(out_task)
    sorted_output_tasks = sorted(output_tasks, key=lambda k: k['task_name'])
    return sorted_output_tasks


def format_timestamp(timestamp):
    """ Standardize Cromwell timestamps to follow the date-time JSON format required by the analysis process schema.

    Args:
        timestamp (str): A datetime string in any format
    Returns:
        formatted_timestamp (str): A datetime string in the format 'YYYY-MM-DDTHH:mm:ss.SSSZ'

    """
    if timestamp:
        d = arrow.get(timestamp)
        formatted_date = d.format('YYYY-MM-DDTHH:mm:ss.SSS')
        return '{}Z'.format(formatted_date)


def get_file_format(path, extension_to_format):
    """Returns the file type of the file at the given path.

    Args:
        path (str): The path to the file.
        extension_to_format (dict): dict mapping file extensions to file types.

    Returns:
        str: A string representing the format of the file, if not applicable, 'unknown' will be returned.
    """

    for ext in extension_to_format:
        if re.search(ext, path):
            file_format = extension_to_format[ext]
            print('file_format: {0}'.format(file_format))
            return file_format
    print('Warning: no known format in the format_map matches file {}'.format(path))
    return 'unknown'


def get_analysis_protocol_core(pipeline_version, **kwargs):
    """Creates protocol_core metadata for analysis_protocol.

    This defines the core entry of the analysis protocol, the protocol_id should be static between runs given a
    specific pipeline_version.

    Args:
        pipeline_version (str): The version of the pipeline, usually provided by the label of the adapter workflow
                                around the analysis workflow.
        **kwargs: Non-required but optional fields for analysis_protocol core. e.g. protocol_name, protocol_description.

    Returns:
        analysis_protocol_core (dict): Dict containing protocol_core metadata required for analysis_protocol.
    """
    analysis_protocol_core = {'protocol_id': pipeline_version}
    for optional_key, optional_val in kwargs.items():
        analysis_protocol_core[optional_key] = optional_val
    return analysis_protocol_core


def get_analysis_protocol_type():
    """Creates protocol_type metadata for analysis_protocol.

    This defines the type of protocol, which refers to the process_type_ontology. The only required key of the dict
    should be 'text', and the value should be the name of a process type being used.

    Returns:
        analysis_protocol_type (dict): The protocol_type metadata for analysis_protocol.
    """
    analysis_protocol_type = {'text': 'analysis'}
    return analysis_protocol_type


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--analysis_id', required=True, help='Cromwell UUID of the analysis workflow.'
    )
    parser.add_argument(
        '--metadata_json',
        required=True,
        help='Path to the JSON obtained from calling Cromwell /metadata for analysis workflow UUID.',
    )
    parser.add_argument(
        '--input_bundles',
        required=True,
        help='A comma-separated list of the DSS bundles used as inputs for the analysis workflow.',
    )
    parser.add_argument(
        '--reference_bundle',
        required=True,
        help='To refer to the DSS resource bundle used for this workflow, once such things exist',
    )
    parser.add_argument(
        '--run_type',
        required=True,
        help='Should always be "run" for now, may be "copy-forward" in some cases in future',
    )
    parser.add_argument(
        '--method',
        required=True,
        help='Supposed to be method store url, for now can be url for wdl in skylab, or the name of'
        ' the analysis workflow.',
    )
    parser.add_argument(
        '--schema_url', required=True, help='URL for retrieving HCA metadata schemas.'
    )
    parser.add_argument(
        '--analysis_process_schema_version',
        required=True,
        help='Version of the metadata schema that the analysis_process conforms to.',
    )
    parser.add_argument(
        '--analysis_protocol_schema_version',
        required=True,
        help='Version of the metadata schema that the analysis_protocol conforms to.',
    )
    parser.add_argument(
        '--pipeline_version',
        required=True,
        help='The version of the pipeline, currently provided by the label of the adapter workflow'
        ' around the analysis workflow.',
    )
    parser.add_argument(
        '--analysis_file_version',
        required=True,
        help='The metadata schema version that the output files(analysis_file) conform to.',
    )
    parser.add_argument(
        '--inputs_file',
        required=True,
        help='Path to tsv file containing info about inputs.',
    )
    parser.add_argument(
        '--outputs_file',
        required=True,
        help='Path to JSON file containing info about outputs.',
    )
    parser.add_argument(
        '--version',
        required=True,
        help='A version (or timestamp) attribute shared across all workflows'
        'within an individual workspace.',
    )
    parser.add_argument(
        '--add_md5s', help='Set to "true" to add md5 checksums to file metadata'
    )
    args = parser.parse_args()

    schema_url = args.schema_url.strip('/')

    # Get metadata for inputs and outputs
    inputs = get_inputs(args.inputs_file)
    with open(args.outputs_file) as f:
        output_urls = f.read().splitlines()
    outputs = get_outputs(
        output_urls=output_urls,
        extension_to_format=EXTENSION_TO_FORMAT,
        schema_url=schema_url,
        analysis_file_version=args.analysis_file_version,
    )

    # Add md5 checksums to input and output metadata if needed
    # See https://github.com/HumanCellAtlas/secondary-analysis/issues/287 for why
    # this is optional.
    if args.add_md5s == 'true':
        client = storage.Client()

        input_urls = get_input_urls(inputs)
        input_url_to_md5 = get_md5s(input_urls, client)
        inputs = add_md5s_to_inputs(inputs, input_url_to_md5)

        output_url_to_md5 = get_md5s(output_urls, client)
        output_name_to_md5 = {
            url.split('/')[-1]: md5 for url, md5 in output_url_to_md5.items()
        }
        outputs = add_md5s_to_outputs(outputs, output_name_to_md5)

    # Write outputs to file
    print('Writing outputs.json to disk...')
    with open('outputs.json', 'w') as f:
        json.dump(outputs, f, indent=2, sort_keys=True)

    # Create analysis_process
    analysis_process = create_analysis_process(
        raw_schema_url=schema_url,
        metadata_file=args.metadata_json,
        analysis_process_schema_version=args.analysis_process_schema_version,
        analysis_id=args.analysis_id,
        inputs=inputs,
        run_type=args.run_type,
        version=args.version,
    )

    # Write analysis_process to file
    print('Writing analysis_process.json to disk...')
    with open('analysis_process.json', 'w') as f:
        json.dump(analysis_process, f, indent=2, sort_keys=True)
    with open('analysis_process_id.txt', 'w') as f:
        f.write()

    # Create analysis_protocol
    analysis_protocol = create_analysis_protocol(
        raw_schema_url=schema_url,
        analysis_protocol_schema_version=args.analysis_protocol_schema_version,
        pipeline_version=args.pipeline_version,
        method=args.method,
        version=args.version,
    )

    # Write analysis_protocol to file
    print('Writing analysis_protocol.json to disk...')
    with open('analysis_protocol.json', 'w') as f:
        json.dump(analysis_protocol, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
