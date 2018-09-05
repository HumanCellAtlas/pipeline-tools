import pytest

from pipeline_tools import optimus_utils
from collections import namedtuple
import random


ManifestEntry = namedtuple('ManifestEntry', ['url'])
BundleFile = namedtuple('BundleFile', ['file_format', 'lane_index', 'read_index', 'manifest_entry'])
# Although file lists are defined in a specific order below (to make it easier to read),
# we want to make sure functionality does not depend on that order, so we randomly shuffle
# the lists before running the tests. Seeding the random number generator here with
# a hard-coded number ensures that the shuffle happens in exactly the same way each time
# the tests are run, so test data is consistent between runs.
r = random.Random(12)


@pytest.fixture
def valid_files_with_index():
    files = [BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
             BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
             BundleFile('fastq.gz', 5, 'index1', ManifestEntry('gs://5/i1.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
             BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz'))]
    r.shuffle(files)
    return files


@pytest.fixture
def valid_files_with_index_dict():
    return {
        5: {
            'read1': 'gs://5/r1.fastq.gz',
            'read2': 'gs://5/r2.fastq.gz',
            'index1': 'gs://5/i1.fastq.gz'
        },
        7: {
            'read1': 'gs://7/r1.fastq.gz',
            'read2': 'gs://7/r2.fastq.gz',
            'index1': 'gs://7/i1.fastq.gz'
        }
    }


@pytest.fixture
def valid_files_no_index():
    files = [BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
             BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz'))]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_one_lane_indexed():
    files = [BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
             BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz')),
             BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz'))]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_one_lane_indexed_dict():
    return {
        5: {
            'read1': 'gs://5/r1.fastq.gz',
            'read2': 'gs://5/r2.fastq.gz'
        },
        7: {
            'read1': 'gs://7/r1.fastq.gz',
            'read2': 'gs://7/r2.fastq.gz',
            'index1': 'gs://7/i1.fastq.gz'
        }
    }


@pytest.fixture
def invalid_files_missing_read1():
    files = [BundleFile('fastq.gz', 5, 'read1', ManifestEntry('gs://5/r1.fastq.gz')),
             BundleFile('fastq.gz', 5, 'read2', ManifestEntry('gs://5/r2.fastq.gz')),
             BundleFile('fastq.gz', 7, 'read2', ManifestEntry('gs://7/r2.fastq.gz'))]
    r.shuffle(files)
    return files


@pytest.fixture
def invalid_files_missing_read2():
    files = [BundleFile('fastq.gz', 7, 'read1', ManifestEntry('gs://7/r1.fastq.gz')),
             BundleFile('fastq.gz', 7, 'index1', ManifestEntry('gs://7/i1.fastq.gz'))]
    r.shuffle(files)
    return files


def test_create_fastq_dict(valid_files_with_index,
                           valid_files_with_index_dict,
                           invalid_files_one_lane_indexed,
                           invalid_files_one_lane_indexed_dict):
    fastq_dict = optimus_utils.create_fastq_dict(valid_files_with_index)
    assert fastq_dict == valid_files_with_index_dict

    fastq_dict = optimus_utils.create_fastq_dict(invalid_files_one_lane_indexed)
    assert fastq_dict == invalid_files_one_lane_indexed_dict


def test_validate_lanes_requires_read1(invalid_files_missing_read1):
    fastq_dict = optimus_utils.create_fastq_dict(invalid_files_missing_read1)
    with pytest.raises(optimus_utils.LaneMissingFileError):
        optimus_utils.validate_lanes(fastq_dict)


def test_validate_lanes_requires_read2(invalid_files_missing_read2):
    fastq_dict = optimus_utils.create_fastq_dict(invalid_files_missing_read2)
    with pytest.raises(optimus_utils.LaneMissingFileError):
        optimus_utils.validate_lanes(fastq_dict)


def test_validate_lanes_rejects_mixing_indexed_and_non_indexed_lanes(invalid_files_one_lane_indexed):
    fastq_dict = optimus_utils.create_fastq_dict(invalid_files_one_lane_indexed)
    with pytest.raises(optimus_utils.LaneMissingFileError):
        optimus_utils.validate_lanes(fastq_dict)


def test_validate_lanes_accepts_lanes_when_all_indexed(valid_files_with_index):
    fastq_dict = optimus_utils.create_fastq_dict(valid_files_with_index)
    optimus_utils.validate_lanes(fastq_dict)


def test_validate_lanes_accepts_lanes_when_none_indexed(valid_files_no_index):
    fastq_dict = optimus_utils.create_fastq_dict(valid_files_no_index)
    optimus_utils.validate_lanes(fastq_dict)


def test_get_fastqs_for_read_index(valid_files_with_index):
    fastq_dict = optimus_utils.create_fastq_dict(valid_files_with_index)
    fastqs = optimus_utils.get_fastqs_for_read_index(fastq_dict, 'read1')
    assert fastqs == ['gs://5/r1.fastq.gz', 'gs://7/r1.fastq.gz']

    fastqs = optimus_utils.get_fastqs_for_read_index(fastq_dict, 'read2')
    assert fastqs == ['gs://5/r2.fastq.gz', 'gs://7/r2.fastq.gz']

    fastqs = optimus_utils.get_fastqs_for_read_index(fastq_dict, 'index1')
    assert fastqs == ['gs://5/i1.fastq.gz', 'gs://7/i1.fastq.gz']
