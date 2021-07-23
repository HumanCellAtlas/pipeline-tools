import uuid
import re


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
    elif(format == "bam" or format == "loom"):
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
