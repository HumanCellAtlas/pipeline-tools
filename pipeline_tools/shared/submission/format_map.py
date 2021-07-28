import arrow
import uuid
import re
import json
from csv import DictReader


EXTENSION_TO_FORMAT = {
    "[.]bam$": "bam",
    "[.]loom$": "loom",
    "[_]metrics$": "metrics",
    "[.]txt$": "txt",
    "[.]fa$": "fasta",
    "[.]fasta$": "fasta",
    "[.]log$": "log",
    "[.]pdf$": "pdf",
    "[.]results$": "results",
    "[.]theta$": "theta",
    "[.]cnt$": "cnt",
    "[.]time$": "time",
    "[.]model$": "model",
    "[.]bai$": "bai",
    "[.]tsv$": "tsv",
    "[.]mtx$": "mtx",
    "[.]h5$": "h5",
    "[.]csv$": "csv",
    "[.]csv.gz$": "csv.gz",
    "[.]zarr": "matrix",
    "[.]npz$": "npz",
    "[.]npy$": "npy",
}

MIME_FORMATS = [
    ('application/vnd.loom', '.loom'),
    ('application/octet-stream', '.bam'),
    ('application/octet-stream', '.bai'),
    ('application/octet-stream', '.fa'),
    ('application/octet-stream', '.fasta')]

NAMESPACE = uuid.UUID('c6591d1d-27bc-4c94-bd54-1b51f8a2456c')


def get_uuid5(sha256):
    return str(uuid.uuid5(NAMESPACE, sha256))


def convert_datetime(creation_time):
    if creation_time.endswith('.000000Z'):
        return creation_time
    return creation_time.replace('Z', '.000000Z')


def get_entity_type(path):
    """Returns the type of file being processed based on the path"""

    format = get_file_format(path)

    if(format == "fasta"):
        return "reference_file"

    elif(format == "bam" or format == "loom" or format == "bai"):
        return "analysis_file"
    return "unknown"


def get_file_format(path):
    """Returns the file type of the file at the given path, according to EXTENSION_TO_FORMAT"""

    for ext in EXTENSION_TO_FORMAT:
        if re.search(ext, path):
            file_format = EXTENSION_TO_FORMAT[ext]
            return file_format

    print('Warning: no known format in the format_map matches file {}'.format(path))
    return 'unknown'


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


def get_metadata(metadata_json):
    f = open(metadata_json)
    data = json.load(f)
    return data


def get_inputs_ss2(inputs, input_ids_inputs, fastq1_inputs, fastq2_inputs=None):
    with open(input_ids_inputs) as f:
        input_ids = [id for id in f]
    input_id_inputs_dict = {'parameter_name': 'input_ids', 'parameter_value': input_ids}
    inputs.append(input_id_inputs_dict)

    with open(fastq1_inputs) as f:
        fastq1_files = [id for id in f]
    fastq1_inputs_dict = {'parameter_name': 'fastq1_input_files', 'parameter_value': fastq1_files}
    inputs.append(fastq1_inputs_dict)

    if fastq2_inputs is not None:
        with open(fastq2_inputs) as f:
            fastq2_files = [id for id in f]
        fastq2_inputs_dict = {'parameter_name': 'fastq2_input_files', 'parameter_value': fastq2_files}
        inputs.append(fastq2_inputs_dict)

    return inputs


def get_workflow_metadata(metadata_json):
    """Load workflow metadata from a JSON file.

    Args:
        metadata_json (str): Path to file containing metadata json for the workflow.

    Returns:
        metadata (dict): A dict consisting of Cromwell workflow metadata information.
    """
    with open(metadata_json) as f:
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
