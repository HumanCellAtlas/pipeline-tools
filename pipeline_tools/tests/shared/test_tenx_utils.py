import pytest

import random
from collections import namedtuple
from pipeline_tools.shared import tenx_utils


ManifestEntry = namedtuple('ManifestEntry', ['url'])
BundleFile = namedtuple(
    'BundleFile', ['file_format', 'lane_index', 'read_index', 'manifest_entry']
)
# Although file lists are defined in a specific order below (to make it easier to read),
# we want to make sure functionality does not depend on that order, so we randomly shuffle
# the lists before running the tests. Seeding the random number generator here with
# a hard-coded number ensures that the shuffle happens in exactly the same way each time
# the tests are run, so test data is consistent between runs.
r = random.Random(12)


@pytest.fixture
def valid_files_with_index():
    files = [
        BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
        BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
        BundleFile('fastq.gz', 5, 'index1', ManifestEntry('gs://5/i1.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
        BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz')),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def valid_files_with_index_dict():
    return {
        5: {
            'read1': ManifestEntry('gs://5/r1.fastq.gz'),
            'read2': ManifestEntry('gs://5/r2.fastq.gz'),
            'index1': ManifestEntry('gs://5/i1.fastq.gz'),
        },
        7: {
            'read1': ManifestEntry('gs://7/r1.fastq.gz'),
            'read2': ManifestEntry('gs://7/r2.fastq.gz'),
            'index1': ManifestEntry('gs://7/i1.fastq.gz'),
        },
    }


@pytest.fixture
def valid_files_no_index():
    files = [
        BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
        BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_one_lane_indexed():
    files = [
        BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
        BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
        BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz')),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_one_lane_indexed_dict():
    return {
        5: {
            'read1': ManifestEntry('gs://5/r1.fastq.gz'),
            'read2': ManifestEntry('gs://5/r2.fastq.gz'),
        },
        7: {
            'read1': ManifestEntry('gs://7/r1.fastq.gz'),
            'read2': ManifestEntry('gs://7/r2.fastq.gz'),
            'index1': ManifestEntry('gs://7/i1.fastq.gz'),
        },
    }


@pytest.fixture
def invalid_files_missing_read1():
    files = [
        BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
        BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
        BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_missing_read2():
    files = [
        BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
        BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz')),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def missing_lane_index_one_set_of_reads():
    files = [
        BundleFile(
            'fastq.gz', None, 'read1', ManifestEntry('gs://somewhere/r1.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'read2', ManifestEntry('gs://somewhere/r2.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'index1', ManifestEntry('gs://somewhere/i1.fastq.gz')
        ),
    ]
    r.shuffle(files)
    return files


@pytest.fixture
def missing_lane_index_multiple_sets_of_reads():
    files = [
        BundleFile(
            'fastq.gz', None, 'read1', ManifestEntry('gs://somewhere/r1.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'read2', ManifestEntry('gs://somewhere/r2.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'index1', ManifestEntry('gs://somewhere/i1.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'read1', ManifestEntry('gs://somewhereelse/r1.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'read2', ManifestEntry('gs://somewhereelse/r2.fastq.gz')
        ),
        BundleFile(
            'fastq.gz', None, 'index1', ManifestEntry('gs://somewhereelse/i1.fastq.gz')
        ),
    ]
    r.shuffle(files)
    return files


def test_create_fastq_dict(
    valid_files_with_index,
    valid_files_with_index_dict,
    invalid_files_one_lane_indexed,
    invalid_files_one_lane_indexed_dict,
):
    fastq_dict = tenx_utils.create_fastq_dict(valid_files_with_index)
    assert fastq_dict == valid_files_with_index_dict

    fastq_dict = tenx_utils.create_fastq_dict(invalid_files_one_lane_indexed)
    assert fastq_dict == invalid_files_one_lane_indexed_dict


def test_validate_lanes_requires_read1(invalid_files_missing_read1):
    fastq_dict = tenx_utils.create_fastq_dict(invalid_files_missing_read1)
    with pytest.raises(tenx_utils.LaneMissingFileError):
        tenx_utils.validate_lanes(fastq_dict)


def test_validate_lanes_requires_read2(invalid_files_missing_read2):
    fastq_dict = tenx_utils.create_fastq_dict(invalid_files_missing_read2)
    with pytest.raises(tenx_utils.LaneMissingFileError):
        tenx_utils.validate_lanes(fastq_dict)


def test_validate_lanes_rejects_mixing_indexed_and_non_indexed_lanes(
    invalid_files_one_lane_indexed
):
    fastq_dict = tenx_utils.create_fastq_dict(invalid_files_one_lane_indexed)
    with pytest.raises(tenx_utils.LaneMissingFileError):
        tenx_utils.validate_lanes(fastq_dict)


def test_validate_lanes_accepts_lanes_when_all_indexed(valid_files_with_index):
    fastq_dict = tenx_utils.create_fastq_dict(valid_files_with_index)
    tenx_utils.validate_lanes(fastq_dict)


def test_validate_lanes_accepts_lanes_when_none_indexed(valid_files_no_index):
    fastq_dict = tenx_utils.create_fastq_dict(valid_files_no_index)
    tenx_utils.validate_lanes(fastq_dict)


def test_create_fastq_dict_reindexes_with_zero_when(
    missing_lane_index_one_set_of_reads
):
    fastq_dict = tenx_utils.create_fastq_dict(missing_lane_index_one_set_of_reads)
    assert 0 in fastq_dict
    assert None not in fastq_dict


def test_create_fastq_dict_raises_error_when(missing_lane_index_multiple_sets_of_reads):
    with pytest.raises(tenx_utils.InsufficientLaneInfoError):
        tenx_utils.create_fastq_dict(missing_lane_index_multiple_sets_of_reads)


def test_get_fastqs_for_read_index(valid_files_with_index):
    fastq_dict = tenx_utils.create_fastq_dict(valid_files_with_index)
    fastqs = tenx_utils.get_fastqs_for_read_index(fastq_dict, 'read1')
    assert fastqs == ['gs://5/r1.fastq.gz', 'gs://7/r1.fastq.gz']

    fastqs = tenx_utils.get_fastqs_for_read_index(fastq_dict, 'read2')
    assert fastqs == ['gs://5/r2.fastq.gz', 'gs://7/r2.fastq.gz']

    fastqs = tenx_utils.get_fastqs_for_read_index(fastq_dict, 'index1')
    assert fastqs == ['gs://5/i1.fastq.gz', 'gs://7/i1.fastq.gz']
