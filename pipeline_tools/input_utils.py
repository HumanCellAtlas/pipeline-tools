from pipeline_tools import dcp_utils
from pipeline_tools.http_requests import HttpRequests


def get_sample_id(metadata, version, sequencing_protocol_id=None):
    """Return the sample id from the given metadata

    Args:
        metadata (dict): metadata related to sample
        version (str): version of the metadata

    Returns:
        String giving the sample id

    Raises:
        NotImplementedError: if version is unsupported
    """
    if version.startswith('4.'):
        return _get_sample_id_v4(metadata)
    elif version.startswith('5.'):
        return _get_sample_id_v5(metadata)
    elif version.startswith('6.'):
        return _get_sample_id_v6(metadata, sequencing_protocol_id)
    else:
        raise NotImplementedError('Only implemented for v4, v5 and v6 metadata')


def _get_sample_id_v6(links_json, sequencing_protocol_id):
    def is_sequencing_protocol_link(link, sequencing_protocol_id):
        return link['destination_id'] == sequencing_protocol_id and link['source_type'] == 'process'

    def is_sample_link(link, process_id):
        return link['source_type'] == 'biomaterial' and link['destination_id'] == process_id

    links = links_json['links']
    sequencing_protocols = list(filter(lambda x: is_sequencing_protocol_link(x, sequencing_protocol_id), links))
    process_id = sequencing_protocols[0]['source_id']
    sample_links = list(filter(lambda x: is_sample_link(x, process_id), links))
    num_links = len(sample_links)
    if num_links == 0:
        raise ValueError('No sample link found')
    elif num_links > 1:
        raise ValueError('Expecting one sample link. {0} found'.format(num_links))
    return sample_links[0]['source_id']


def _get_sample_id_v5(links_json):
    """Return sample id from links json

    Args:
        links_json (dict): links json

    Returns:
        String giving sample id

    Raises:
        ValueError: if 0 or more than 1 sample link found
    """
    links = links_json['links']
    def is_sample_link(link):
        return link['source_type'] == 'biomaterial' and link['destination_type'] == 'sequencing_process'
    sample_links = list(filter(lambda x: is_sample_link(x), links))
    num_links = len(sample_links)
    if num_links == 0:
        raise ValueError('No sample link found')
    elif num_links > 1:
        raise ValueError('Expecting one sample link. {0} found'.format(num_links))
    return sample_links[0]['source_id']


def _get_sample_id_v4(assay_json):
    """Return sample id from assay json"""
    return assay_json["has_input"]


def get_input_metadata_file_uuid(manifest_files, version):
    """Get the uuid of the file containing metadata about pipeline input files,
    e.g. assay.json in v4

    Args:
        manifest_files (dict): file metadata
        version (str): metadata version

    Returns:
        String giving the uuid of the input metadata file

    Raises:
        NotImplementedError: if version is unsupported
    """
    # if version.startswith('5.'):
    #     return _get_input_metadata_file_uuid_v5(manifest_files)
    if version.startswith('4.'):
        return _get_input_metadata_file_uuid_v4(manifest_files)
    else:
        return _get_input_metadata_file_uuid_v5(manifest_files)
    # else:
    #     raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_input_metadata_file_uuid_v5(manifest_files):
    """Get the uuid of the files.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'file.json')


def _get_input_metadata_file_uuid_v4(manifest_files):
    """Get the uuid of the assay.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'assay.json')


def get_sample_id_file_uuid(manifest_files, version):
    """Get the uuid of the file containing the sample id,
    e.g. assay.json in v4

    Args:
        manifest_files (dict): file metadata
        version (str): version of metadata

    Returns:
        String giving uuid of file containing sample id

    Raises:
        NotImplementedError: if metadata version is unsupported
    """
    # if version.startswith('5.'):
    #     return _get_sample_id_file_uuid_v5_or_higher(manifest_files)
    if version.startswith('4.'):
        return _get_sample_id_file_uuid_v4(manifest_files)
    else:
        return _get_sample_id_file_uuid_v5_or_higher(manifest_files)
        # raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_sample_id_file_uuid_v5_or_higher(manifest_files):
    """Get the uuid of the links.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'links.json')


def _get_sample_id_file_uuid_v4(manifest_files):
    """Get the uuid of the assay.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'assay.json')


def get_smart_seq_2_fastq_names(metadata, version):
    """Get the fastq file names from the given metadata

    Args:
        metadata (dict): file metadata
        version (str): the metadata version

    Returns:
        tuple of two strings representing the two fastq names

    Raises:
        NotImplementedError: if metadata version is unsupported
    """
    version_prefix = int(version.split('.', 1)[0])
    if version_prefix >= 5:
        return _get_smart_seq_2_fastq_names_v5_or_higher(metadata)
    elif version_prefix == 4:
        return _get_smart_seq_2_fastq_names_v4(metadata)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_smart_seq_2_fastq_names_v5_or_higher(files_json):
    """Returns fastq file names

    Args:
        files_json (dict): file metadata
    
    Returns:
        tuple of two strings representing the two fastq names
    """
    index_to_name = {}
    for f in files_json['files']:
        index = f['content']['read_index']
        file_name = f['content']['file_core']['file_name']
        index_to_name[index] = file_name
    return index_to_name['read1'], index_to_name['read2']


def _get_smart_seq_2_fastq_names_v4(assay_json):
    """Returns fastq file names

    Args:
        assay_json (dict): metadata about the assay
    
    Returns:
        tuple of two strings representing the two fastq names
    """
    fastq_1_name = assay_json["content"]["seq"]["lanes"][0]["r1"]
    fastq_2_name = assay_json["content"]["seq"]["lanes"][0]["r2"]
    return fastq_1_name, fastq_2_name


def get_optimus_lanes(metadata_json, version):
    """Get the lane metadata

    Args:
        metadata_json (dict): metadata
        version (str): the metadata version

    Returns:
        Dict of lane metadata 

    Raises:
        NotImplementedError: if metadata version is unsupported
    """
    if version.startswith('4.'):
        return _get_optimus_lanes_v4(metadata_json)
    else:
        raise NotImplementedError('Only implemented for v4 metadata')


def _get_optimus_lanes_v4(assay_json):
    """Return the lane metadata from the assay json"""
    lanes = assay_json['content']['seq']['lanes']
    return lanes


def get_optimus_inputs(lanes, manifest_files):
    """Returns metadata for optimus input files

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


def is_v5_or_higher(name_to_meta):
    """Return true if schema of bundle metadata is at least 5.0.0

    Args:
        name_to_meta (dict): mapping file names to file metadata

    Returns:
        True if bundle metadata schema is at least 5, False otherwise

    Raises:
        ValueError: if version can't be determined
    """
    if 'assay.json' in name_to_meta:
        return False
    elif 'file.json' in name_to_meta:
        return True
    else:
        raise ValueError('No assay.json and no file.json. Cannot determine metadata schema version.')


def detect_schema_version(file_json):
    """Return the bundle's metadata schema version

    Args:
        file_json (dict): file metadata

    Returns:
        String giving the metadata schema version

    Raises:
        ValueError: if bundle contains no files
    """
    files = file_json['files']
    if len(files) == 0:
        raise ValueError('No files in bundle')
    schema_url = files[0]['content']['describedBy']
    version = schema_url.split('/')[-2]
    return version


def get_metadata_to_process(manifest_files, dss_url, is_v5_or_higher, http_requests):
    """Return the metadata json that we need to parse to set up pipeline inputs

    Args:
        manifest_files (dict): file metadata
        dss_url (str): the url of the DCP Data Storage Service
        is_v5_or_higher (bool): whether metadata is v5 or higher
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        Tuple of three items: inputs metadata dict, sample metadata dict, schema version string

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    if not is_v5_or_higher:
        schema_version = '4.x'
        inputs_metadata_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'assay.json')
        inputs_metadata_json = dcp_utils.get_file_by_uuid(inputs_metadata_file_uuid, dss_url, http_requests)
        sample_id_file_json = inputs_metadata_json
    else:
        sample_id_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'links.json')
        inputs_metadata_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'file.json')
        inputs_metadata_json = dcp_utils.get_file_by_uuid(inputs_metadata_file_uuid, dss_url, http_requests)
        schema_version = detect_schema_version(inputs_metadata_json)
        sample_id_file_json = dcp_utils.get_file_by_uuid(sample_id_file_uuid, dss_url, http_requests)
    return inputs_metadata_json, sample_id_file_json, schema_version


def create_ss2_input_tsv(uuid, version, dss_url):
    """Create TSV of Smart-seq2 inputs

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the url for the DCP Data Storage Service

    Returns:
        TSV of cloud paths for the input files

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond the timeout
    """
    fastq_1_url, fastq_2_url, sample_id = _get_content_for_ss2_input_tsv(uuid, version, dss_url, HttpRequests())

    print("Creating input map")
    with open("inputs.tsv", "w") as f:
        f.write("fastq_1\tfastq_2\tsample_id\n")
        f.write("{0}\t{1}\t{2}\n".format(fastq_1_url, fastq_2_url, sample_id))
    print("Wrote input map")


def _get_content_for_ss2_input_tsv(uuid, version, dss_url, http_requests):
    """Gather the necessary metadata for the ss2 input tsv

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the url for the DCP Data Storage Service
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        tuple of three strings: url for fastq 1, url for fastq 2, sample id

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout
    """
    # Get bundle manifest
    print("Getting bundle manifest for id {0}, version {1}".format(uuid, version))
    manifest = dcp_utils.get_manifest(uuid, version, dss_url, http_requests)
    manifest_files = dcp_utils.get_manifest_file_dicts(manifest)

    inputs_metadata_json, sample_id_file_json, schema_version = get_metadata_to_process(
        manifest_files, dss_url, is_v5_or_higher(manifest_files['name_to_meta']), http_requests)

    protocol_uuid = dcp_utils.get_file_uuid(manifest_files, 'protocol.json')
    protocol_json = dcp_utils.get_file_by_uuid(protocol_uuid, dss_url, http_requests)
    sequencing_protocol = [protocol for protocol in protocol_json['protocols'] if 'sequencing_protocol' in protocol['content']['describedBy']]
    sequencing_protocol_id = sequencing_protocol[0]['hca_ingest']['document_id']

    sample_id = get_sample_id(sample_id_file_json, schema_version, sequencing_protocol_id)
    fastq_1_name, fastq_2_name = get_smart_seq_2_fastq_names(inputs_metadata_json, schema_version)
    fastq_1_url = dcp_utils.get_file_url(manifest_files, fastq_1_name)
    fastq_2_url = dcp_utils.get_file_url(manifest_files, fastq_2_name)

    return fastq_1_url, fastq_2_url, sample_id


def create_optimus_input_tsv(uuid, version, dss_url):
    """Create TSV of Optimus inputs

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
    r1, r2, i1 = get_optimus_inputs(lanes, manifest_files)
    fastq_inputs = [list(i) for i in zip(r1, r2, i1)]
    print(fastq_inputs)

    with open('fastq_inputs.tsv', 'w') as f:
        for line in fastq_inputs:
            f.write('\t'.join(line) +'\n')
        print('Writing sample ID to inputs.tsv')
        f.write('{0}'.format(sample_id))

    print('Wrote input map')
