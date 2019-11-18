"""Utilities for preparing inputs for 10x pipelines, e.g. optimus and cellranger.
"""


def create_fastq_dict(fastq_files):
    """Create dictionary mapping each lane to the corresponding fastq files.

    Example output:
    {
        1: {
            'read1': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane1/read1_fastq.gz'...),
            'read2': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane1/read2_fastq.gz'...),
            'index1': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane1/index1_fastq.gz'...)
        },
        2: {
            'read1': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane2/read1_fastq.gz'...),
            'read2': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane2/read2_fastq.gz'...),
            'index1': humancellatlas.data.metadata.api.ManifestEntry(url='gs://path/to/lane2/index1_fastq.gz'...)
        }
    }

    Args:
        fastq_files (List[humancellatlas.data.metadata.File]): list of file metadata objects

    Returns:
        lane_to_fastqs (Dict[Dict[str, humancellatlas.data.metadata.api.ManifestEntry]]): dict mapping flow cell
                        lanes to a dict of read index to its bundle manifest entry
    """
    lane_to_fastqs = {}
    for file in fastq_files:
        lane = file.lane_index
        if lane is None:
            lane = 0
        if lane in lane_to_fastqs:
            if file.read_index in lane_to_fastqs[lane]:
                raise InsufficientLaneInfoError(
                    'There are multiple sets of reads, but no lane index. '
                    'Cannot properly group reads for analysis.'
                )
        else:
            lane_to_fastqs[lane] = {}
        lane_to_fastqs[lane][file.read_index] = file.manifest_entry

    return lane_to_fastqs


def get_fastqs_for_read_index(lane_to_fastqs, read_index):
    """Get a list of fastq urls for the given read index

    Args:
        lane_to_fastqs (Dict[Dict[str, str]]): dict of dict mapping each lane to a dict of read index to fastq url
        read_index (str): the read_index to filter by, e.g. "read1", "read2", or "index1"

    Returns:
        fastq_urls (list): list of fastq urls
    """
    lanes = sorted(lane_to_fastqs.keys())
    fastq_urls = []
    for lane in lanes:
        if read_index in lane_to_fastqs[lane]:
            manifest_entry = lane_to_fastqs[lane][read_index]
            fastq_urls.append(manifest_entry.url)
    return fastq_urls


def validate_lanes(lane_to_fastqs):
    """Verify that every lane has both read1 and read2 and that either all or none have an index.

    Args:
        lane_to_fastqs (dict of dict): dict mapping each lane to a dict of read index to fastq url

    Raises:
        LaneMissingFileError: if a file is missing for a lane

    Returns:
        None
    """
    # Create dictionaries of lane to bool indicating whether lane has file for read1, read2, index1.
    # These are used to help validate that we have all necessary fastqs and to help generate
    # error messages when any files are missing.
    #
    # For example, given this input:
    #
    # lane_to_fastqs = {
    #   3: {
    #       'read1': humancellatlas.data.metadata.api.ManifestEntry(url='foo'...),
    #       'read2': humancellatlas.data.metadata.api.ManifestEntry(url='bar'...),
    #       'index1': humancellatlas.data.metadata.api.ManifestEntry(url='baz'...)
    #       },
    #   4: {
    #       'read1': humancellatlas.data.metadata.api.ManifestEntry(url='foo'...),
    #       'index1': humancellatlas.data.metadata.api.ManifestEntry(url='baz'...)
    #       }
    # }
    #
    # ...the three dicts created would be:
    #
    # lane_to_read1 = {
    #      3: True
    #      4: True
    # }
    #
    # lane_to_read2 = {
    #      3: True
    #      4: False
    # }
    #
    # lane_to_index1 = {
    #      3: True
    #      4: True
    # }
    lane_to_read1 = {}
    lane_to_read2 = {}
    lane_to_index1 = {}
    lanes = sorted(lane_to_fastqs.keys())
    for lane in lanes:
        read_index_to_url = lane_to_fastqs[lane]
        lane_to_read1[lane] = True if 'read1' in read_index_to_url else False
        lane_to_read2[lane] = True if 'read2' in read_index_to_url else False
        lane_to_index1[lane] = True if 'index1' in read_index_to_url else False

    error = False
    # All lanes must have read1 and read2
    if False in lane_to_read1.values() or False in lane_to_read2.values():
        error = True
    # If any index files are provided, they must be provided for all lanes
    if True in lane_to_index1.values() and False in lane_to_index1.values():
        error = True

    # If there's an error, generate error message and raise error
    if error:
        msg = 'One or more lanes is missing a fastq file.\n'
        for lane in lanes:
            format_str = 'Lane {0}: read1: {1}, read2: {2}, index1: {3}\n'
            msg += format_str.format(
                lane, lane_to_read1[lane], lane_to_read2[lane], lane_to_index1[lane]
            )
        raise LaneMissingFileError(msg)


class LaneMissingFileError(Exception):
    pass


class InsufficientLaneInfoError(Exception):
    pass


class UnsupportedTenXChemistryError(Exception):
    pass
