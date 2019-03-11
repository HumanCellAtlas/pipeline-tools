import os
import functools
import typing
from concurrent.futures import ThreadPoolExecutor
from humancellatlas.data.metadata.api import Bundle, CellSuspension

from pipeline_tools import dcp_utils, optimus_utils
from pipeline_tools.http_requests import HttpRequests


class MoreThanOneCellSuspensionError(Exception):
    pass


def get_sample_id(bundle):
    """Return the sample id from the given bundle.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        sample_id (str): String giving the sample id
    """
    sample_id = str(bundle.sequencing_input[0].document_id)
    return sample_id


def get_expected_cell_count(bundle):
    """Return the total estimated cells from the given bundle, otherwise use the same default value as CellRanger
    (3000 cells): https://support.10xgenomics.com/single-cell-gene-expression/software/pipelines/2.1/using/count

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        total_estimated_cells (int): Int giving the total number of estimated cells

    Raises:
        MoreThanOneCellSuspensionError: if the data bundle contains more than one cell_suspension.json file
    """
    cell_suspension = [f for f in bundle.biomaterials.values() if isinstance(f, CellSuspension)]
    n_cell_suspension = len(cell_suspension)
    if n_cell_suspension != 1:
        raise MoreThanOneCellSuspensionError('The data bundle should contain exactly 1 cell_suspension.json file, ' +
                                             'not {}'.format(n_cell_suspension))
    default_estimated_cells = 3000
    total_estimated_cells = cell_suspension[0].total_estimated_cells
    return int(total_estimated_cells) if total_estimated_cells else default_estimated_cells


def get_urls_to_files_for_ss2(bundle):
    """Return the direct urls to the input fastq files for SmartSeq2 pipeline.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
         tuple: A tuple consisting of the url to the input fastq files, which are corresponding to read index 1 and 2
         respectively.
    """
    fastq_1_url = fastq_2_url = None
    sequence_files = bundle.sequencing_output

    for sf in sequence_files:
        if fastq_1_url and fastq_2_url:
            return fastq_1_url, fastq_2_url  # early termination
        if sf.read_index == 'read1':
            fastq_1_url = sf.manifest_entry.url
        if sf.read_index == 'read2':
            fastq_2_url = sf.manifest_entry.url
    return fastq_1_url, fastq_2_url


def download_file(item, dss_url, http_requests=HttpRequests()):
    """Download the metadata for a given bundle from the HCA data store (DSS).

    This function borrows a lot of existing code from the `metadata-api` code base for consistency,
    and this won't be required after migrating to use the HCA DSS Python API `dss_client` directly.

    Args:
        item (typing.ItemsView): A dictionary's ItemsView object consisting of file_name and the manifest_entry.
        dss_url (str): The url for the DCP Data Storage Service.
        http_requests (HttpRequests): The HttpRequests object to use.
    """
    file_name, manifest_entry = item
    file_uuid = manifest_entry['uuid']
    return file_name, dcp_utils.get_file_by_uuid(file_id=file_uuid, dss_url=dss_url, http_requests=http_requests)


def get_metadata_files(metadata_files_dict, dss_url, num_workers=None):
    """Get the dictionary mapping the file name of each metadata file in the bundle to the JSON contents of that file.

    This function by default uses concurrent threads to accelerate the communication with Data Store service.

    Args:
        metadata_files_dict (dict): A dictionary maps filename to indexed file content among the bundle manifest,
                                    this will only be used for preparing the metadata_files dictionary.
        dss_url (str): The url for the DCP Data Storage Service.
        num_workers(int or None): The size of the thread pool to use for downloading metadata files in parallel.
                     If None, the default pool size will be used, typically a small multiple of the number of cores
                     on the system executing this function. If 0, no thread pool will be used and all files will be
                     downloaded sequentially by the current thread.

    Returns:
        metadata_files (dict): A dictionary mapping the file name of each metadata file in the bundle to the JSON
                               contents of that file.
    """
    if num_workers == 0:
        metadata_files = dict(
                map(functools.partial(download_file, dss_url=dss_url, http_requests=HttpRequests()),
                    metadata_files_dict.items())
        )
    else:
        with ThreadPoolExecutor(num_workers) as tpe:
            metadata_files = dict(
                    tpe.map(functools.partial(download_file, dss_url=dss_url, http_requests=HttpRequests()),
                            metadata_files_dict.items())
            )
    return metadata_files


def get_bundle_metadata(uuid, version, dss_url, http_requests):
    """Factory function to create a `humancellatlas.data.metadata.Bundle` object from bundle information and manifest.

    Args:
        bundle_uuid (str): The bundle uuid.
        bundle_version (str): The bundle version.
        dss_url (str): Url of Data Storage System to query
        http_requests (HttpRequests): An HttpRequests object.

    Returns:
        humancellatlas.data.metadata.Bundle: A bundle metadata object.
    """
    manifest = dcp_utils.get_manifest(bundle_uuid=uuid,
                                      bundle_version=version,
                                      dss_url=dss_url,
                                      http_requests=http_requests)['bundle']['files']

    metadata_files_dict = {f['name']: f for f in manifest if f['indexed']}
    metadata_files = get_metadata_files(metadata_files_dict=metadata_files_dict, dss_url=dss_url)

    return Bundle(uuid=uuid, version=version, manifest=manifest, metadata_files=metadata_files)


def _get_content_for_ss2_input_tsv(bundle_uuid, bundle_version, dss_url, http_requests):
    """Gather the necessary metadata for the ss2 input tsv.

    Args:
        bundle_uuid (str): the bundle uuid.
        bundle_version (str): the bundle version.
        dss_url (str): the url for the DCP Data Storage Service.
        http_requests (HttpRequests): the HttpRequests object to use.

    Returns:
        tuple: tuple of three strings; url for fastq 1, url for fastq 2, sample id

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout
    """

    print("Getting bundle manifest for id {0}, version {1}".format(bundle_uuid, bundle_version))
    primary_bundle = get_bundle_metadata(uuid=bundle_uuid,
                                         version=bundle_version,
                                         dss_url=dss_url,
                                         http_requests=http_requests)

    sample_id = get_sample_id(primary_bundle)
    fastq_1_url, fastq_2_url = get_urls_to_files_for_ss2(primary_bundle)
    return fastq_1_url, fastq_2_url, sample_id


def create_ss2_input_tsv(bundle_uuid, bundle_version, dss_url, input_tsv_name='inputs.tsv'):
    """Create TSV of Smart-seq2 inputs.

    Args:
        bundle_uuid (str): The bundle uuid
        bundle_version (str): The bundle version
        dss_url (str): The url for the DCP Data Storage Service
        input_tsv_name (str): The file name of the input TSV file. By default, it's set to 'inputs.tsv',
                              which will be consumed by the pipelines.

    Returns:
        None: this function will write the TSV file of cloud paths for the input files.

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond the timeout
    """
    fastq_1_url, fastq_2_url, sample_id = _get_content_for_ss2_input_tsv(bundle_uuid, bundle_version, dss_url,
                                                                         HttpRequests())

    print('Creating input map')
    with open(input_tsv_name, 'w') as f:
        f.write('fastq_1\tfastq_2\tsample_id\n')
        f.write('{0}\t{1}\t{2}\n'.format(fastq_1_url, fastq_2_url, sample_id))
    print('Wrote input map to disk.')


# TODO: Rename this function since it no longer creates a tsv file
def create_optimus_input_tsv(uuid, version, dss_url):
    """Create TSV of Optimus inputs

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the DCP Data Storage Service

    Returns:
        TSV of input file cloud paths

    Raises:
        optimus_utils.LaneMissingFileError if any fastqs are missing
    """
    # Get bundle manifest
    print('Getting bundle manifest for id {0}, version {1}'.format(uuid, version))
    primary_bundle = get_bundle_metadata(uuid=uuid, version=version, dss_url=dss_url, http_requests=HttpRequests())

    # Parse inputs from metadata
    print('Gathering fastq inputs')
    fastq_files = [f for f in primary_bundle.files.values() if f.file_format == 'fastq.gz']
    lane_to_fastqs = optimus_utils.create_fastq_dict(fastq_files)

    # Stop if any fastqs are missing
    optimus_utils.validate_lanes(lane_to_fastqs)

    r1_urls = optimus_utils.get_fastqs_for_read_index(lane_to_fastqs, 'read1')
    r2_urls = optimus_utils.get_fastqs_for_read_index(lane_to_fastqs, 'read2')
    i1_urls = optimus_utils.get_fastqs_for_read_index(lane_to_fastqs, 'index1')

    print('Writing r1.txt, r2.txt, and i1.txt')
    with open('r1.txt', 'w') as f:
        for url in r1_urls:
            f.write(url + '\n')
    with open('r2.txt', 'w') as f:
        for url in r2_urls:
            f.write(url + '\n')
    with open('i1.txt', 'w') as f:
        for url in i1_urls:
            f.write(url + '\n')
    with open('lanes.txt', 'w') as f:
        lane_numbers = sorted(lane_to_fastqs.keys())
        for l in lane_numbers:
            f.write(str(l))

    sample_id = get_sample_id(primary_bundle)
    print('Writing sample ID to sample_id.txt')
    with open('sample_id.txt', 'w') as f:
        f.write('{0}'.format(sample_id))

    print('Finished writing files')


def get_cellranger_input_files(uuid, version, dss_url):
    """ Get inputs for cellranger count workflow

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the DCP Data Storage Service

    Returns:
        None

    Raises:
        optimus_utils.LaneMissingFileError: if any fastqs are missing
    """
    # Get bundle manifest
    print('Getting bundle manifest for id {0}, version {1}'.format(uuid, version))
    primary_bundle = get_bundle_metadata(uuid=uuid, version=version, dss_url=dss_url, http_requests=HttpRequests())

    sample_id = get_sample_id(primary_bundle)
    print('Writing sample ID to sample_id.txt')
    with open('sample_id.txt', 'w') as f:
        f.write('{0}'.format(sample_id))

    total_estimated_cells = get_expected_cell_count(primary_bundle)
    print('Writing total estimated cells to expect_cells.txt')
    with open('expect_cells.txt', 'w') as f:
        f.write('{0}'.format(total_estimated_cells))

    # Parse inputs from metadata
    print('Gathering fastq inputs')
    fastq_files = [f for f in primary_bundle.files.values() if f.file_format == 'fastq.gz']
    lane_to_fastqs = optimus_utils.create_fastq_dict(fastq_files)

    # Stop if any fastqs are missing
    optimus_utils.validate_lanes(lane_to_fastqs)

    read_indices = {
        'read1': 'R1',
        'read2': 'R2',
        'index1': 'I1'
    }
    fastq_urls = []
    fastq_names = []

    for lane, reads in lane_to_fastqs.items():
        for read_index, url in reads.items():
            new_file_name = '{}_S1_L00{}_{}_001.fastq.gz'.format(sample_id, str(lane), read_indices[read_index])
            fastq_names.append(new_file_name)
            fastq_urls.append(url)

    with open('fastqs.txt', 'w') as f:
        for url in fastq_urls:
            f.write(url + '\n')

    with open('fastq_names.txt', 'w') as f:
        for name in fastq_names:
            f.write(name + '\n')

    print('Finished writing files')
