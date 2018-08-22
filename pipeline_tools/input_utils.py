import functools
import typing
from concurrent.futures import ThreadPoolExecutor
from humancellatlas.data.metadata import Bundle

from pipeline_tools import dcp_utils
from pipeline_tools.http_requests import HttpRequests


def get_bundle_object(bundle_uuid, bundle_version, manifest, metadata_files):
    """Factory function to create a `humancellatlas.data.metadata.Bundle` object from bundle information and manifest.

    Args:
        bundle_uuid (str): The bundle uuid.
        bundle_version (str): The bundle version.
        manifest (list): A list of dictionaries represent the manifest JSON file.
        metadata_files (dict): A dictionary mapping the file name of each metadata file in the bundle to the JSON
                               contents of that file.

    Returns:
        humancellatlas.data.metadata.Bundle: A Bundle object contains all of the necessary information.
    """
    return Bundle(uuid=bundle_uuid,
                  version=bundle_version,
                  manifest=manifest,
                  metadata_files=metadata_files)


def get_sample_id(bundle):
    """Return the sample id from the given bundle.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        sample_id (str): String giving the sample id
    """
    cell_suspension = bundle.sequencing_input[0]
    sample_id = str(cell_suspension.document_id)
    return sample_id


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
    manifest = dcp_utils.get_manifest(bundle_uuid=bundle_uuid,
                                      bundle_version=bundle_version,
                                      dss_url=dss_url,
                                      http_requests=http_requests)['bundle']['files']

    metadata_files_dict = {f['name']: f for f in manifest if f['indexed']}
    metadata_files = get_metadata_files(metadata_files_dict=metadata_files_dict, dss_url=dss_url)

    # construct the bundle object to get required fields
    ss2_primary_bundle = get_bundle_object(bundle_uuid=bundle_uuid,
                                           bundle_version=bundle_version,
                                           manifest=manifest,
                                           metadata_files=metadata_files)

    sample_id = get_sample_id(ss2_primary_bundle)
    fastq_1_url, fastq_2_url = get_urls_to_files_for_ss2(ss2_primary_bundle)
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


def _get_optimus_inputs(lanes, manifest_files):
    """Returns metadata for optimus input files
    FIXME: Update this function with the `metadata-api`, until then this function is broken and won't work!

    Args:
        lanes (dict): lane metadata
        manifest_files (dict): file metadata

    Returns:
        Three lists of urls, representing fastqs for r1, r2, and i1, respectively.
    In each list, the first item is for the first lane, the second item is for the second lane, etc.
    """
    r1 = [manifest_files['name_to_meta'][lane['r1']]['url'] for lane in lanes]
    r2 = [manifest_files['name_to_meta'][lane['r2']]['url'] for lane in lanes]
    i1 = [manifest_files['name_to_meta'][lane['i1']]['url'] for lane in lanes]

    return r1, r2, i1


def create_optimus_input_tsv(uuid, version, dss_url):
    """Create TSV of Optimus inputs
    FIXME: Update this function with the `metadata-api`, until then this function is broken and won't work!

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the DCP Data Storage Service

    Returns:
        TSV of input file cloud paths
    """
    # Get bundle manifest
    print('Getting bundle manifest for id {0}, version {1}'.format(uuid, version))
    manifest = dcp_utils.get_manifest(uuid, version, dss_url)
    manifest_files = dcp_utils.get_manifest_file_dicts(manifest)

    inputs_metadata_json, sample_id_file_json, schema_version = get_metadata_to_process(
            manifest_files, dss_url, is_v5_or_higher(manifest_files['name_to_meta']))

    # Parse inputs from metadata and write to fastq_inputs
    print('Writing fastq inputs to fastq_inputs.tsv')
    sample_id = get_sample_id(sample_id_file_json, schema_version)
    lanes = get_optimus_lanes(inputs_metadata_json, schema_version)
    r1, r2, i1 = _get_optimus_inputs(lanes, manifest_files)
    fastq_inputs = [list(i) for i in zip(r1, r2, i1)]
    print(fastq_inputs)

    with open('fastq_inputs.tsv', 'w') as f:
        for line in fastq_inputs:
            f.write('\t'.join(line) + '\n')
        print('Writing sample ID to inputs.tsv')
        f.write('{0}'.format(sample_id))

    print('Wrote input map')
