import uuid


EXTENSION_TO_FORMAT = {
    "[.]bam$": "bam",
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

NAMESPACE = uuid.UUID('c6591d1d-27bc-4c94-bd54-1b51f8a2456c')
