import uuid


EXTENSION_TO_FORMAT = {
    "[.]bam$": "bam",
    "[.]loom$": "loom",
    "[_]metrics$": "metrics",
    "[.]txt$": "txt",
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
    ('application/octet-stream', '.fa')]

NAMESPACE = uuid.UUID('c6591d1d-27bc-4c94-bd54-1b51f8a2456c')


def get_uuid5(sha256):
    return str(uuid.uuid5(NAMESPACE, sha256))


def convert_datetime(creation_time):
    return creation_time.replace('Z', '.000000Z')
