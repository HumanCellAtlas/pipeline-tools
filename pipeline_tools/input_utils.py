from pipeline_tools import dcp_utils


def get_sample_id(metadata, version):
    """Return the sample id from the given metadata"""
    if version.startswith('4.'):
        return _get_sample_id_v4(metadata)
    elif version.startswith('5.'):
        return _get_sample_id_v5(metadata)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_sample_id_v5(links_json):
    """Return sample id from links json"""
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
    e.g. assay.json in v4"""
    if version.startswith('5.'):
        return _get_input_metadata_file_uuid_v5(manifest_files)
    elif version.startswith('4.'):
        return _get_input_metadata_file_uuid_v4(manifest_files)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_input_metadata_file_uuid_v5(manifest_files):
    """Get the uuid of the files.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'file.json')


def _get_input_metadata_file_uuid_v4(manifest_files):
    """Get the uuid of the assay.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'assay.json')


def get_sample_id_file_uuid(manifest_files, version):
    """Get the uuid of the file containing the sample id,
    e.g. assay.json in v4"""
    if version.startswith('5.'):
        return _get_sample_id_file_uuid_v5(manifest_files)
    elif version.startswith('4.'):
        return _get_sample_id_file_uuid_v4(manifest_files)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_sample_id_file_uuid_v5(manifest_files):
    """Get the uuid of the links.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'links.json')


def _get_sample_id_file_uuid_v4(manifest_files):
    """Get the uuid of the assay.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'assay.json')


def get_smart_seq_2_fastq_names(metadata, version):
    """Get the fastq file names from the given metadata"""
    if version.startswith('5.'):
        return _get_smart_seq_2_fastq_names_v5(metadata)
    elif version.startswith('4.'):
        return _get_smart_seq_2_fastq_names_v4(metadata)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_smart_seq_2_fastq_names_v5(files_json):
    """Return fastq file names from files json"""
    index_to_name = {}
    for f in files_json['files']:
        index = f['content']['read_index']
        file_name = f['content']['file_core']['file_name']
        index_to_name[index] = file_name
    return index_to_name['read1'], index_to_name['read2']


def _get_smart_seq_2_fastq_names_v4(assay_json):
    """Return fastq file names from assay json"""
    fastq_1_name = assay_json["content"]["seq"]["lanes"][0]["r1"]
    fastq_2_name = assay_json["content"]["seq"]["lanes"][0]["r2"]
    return fastq_1_name, fastq_2_name


def get_optimus_lanes(metadata_json, version):
    """Get the lane metadata"""
    if version.startswith('4.'):
        return _get_optimus_lanes_v4(metadata_json)
    else:
        raise NotImplementedError('Only implemented for v4 metadata')


def _get_optimus_lanes_v4(assay_json):
    """Return the lane metadata from the assay json"""
    lanes = assay_json['content']['seq']['lanes']
    return lanes


def get_optimus_inputs(lanes, manifest_files):
    """Return three lists of urls, representing fastqs for r1, r2, and i1, respectively.
    In each list, the first item is for the first lane, the second item is for the second lane, etc.    
    """
    r1 = [manifest_files['name_to_meta'][lane['r1']]['url'] for lane in lanes]
    r2 = [manifest_files['name_to_meta'][lane['r2']]['url'] for lane in lanes]
    i1 = [manifest_files['name_to_meta'][lane['i1']]['url'] for lane in lanes]

    return r1, r2, i1


def is_v5_or_higher(name_to_meta):
    """Return true if schema of bundle metadata is at least 5.0.0"""
    if 'assay.json' in name_to_meta:
        return False
    elif 'file.json' in name_to_meta:
        return True
    else:
        raise ValueError('No assay.json and no process.json. Cannot determine metadata schema version.')


def detect_schema_version(file_json):
    """Return the bundle's metadata schema version"""
    files = file_json['files']
    if len(files) == 0:
        raise ValueError('No files in bundle')
    schema_url = files[0]['content']['describedBy']
    version = schema_url.split('/')[-2]
    return version


def get_metadata_to_process(manifest_files, dss_url, is_v5_or_higher):
    """Return the metadata json that we need to parse to set up pipeline inputs"""
    if not is_v5_or_higher:
        schema_version = '4.x'
        sample_id_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'sample.json')
        inputs_metadata_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'assay.json')
        inputs_metadata_json = dcp_utils.get_file_by_uuid(inputs_metadata_file_uuid, dss_url)
        sample_id_file_json = inputs_metadata_json
    else:
        sample_id_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'links.json')
        inputs_metadata_file_uuid = dcp_utils.get_file_uuid(manifest_files, 'file.json')
        inputs_metadata_json = dcp_utils.get_file_by_uuid(inputs_metadata_file_uuid, dss_url)
        schema_version = detect_schema_version(inputs_metadata_json)
        sample_id_file_json = dcp_utils.get_file_by_uuid(sample_id_file_uuid, dss_url)
    return inputs_metadata_json, sample_id_file_json, schema_version


def create_ss2_input_tsv(uuid, version, dss_url, retry_seconds, timeout_seconds):
    """Create tsv of Smart-seq2 inputs"""
    # Get bundle manifest
    print("Getting bundle manifest for id {0}, version {1}".format(uuid, version))
    manifest = dcp_utils.get_manifest(uuid, version, dss_url, timeout_seconds, retry_seconds)
    manifest_files = dcp_utils.get_manifest_file_dicts(manifest)

    inputs_metadata_json, sample_id_file_json, schema_version = get_metadata_to_process(
        manifest_files, dss_url, is_v5_or_higher(manifest_files['name_to_meta']))

    sample_id = get_sample_id(sample_id_file_json, schema_version)
    fastq_1_name, fastq_2_name = get_smart_seq_2_fastq_names(inputs_metadata_json, schema_version)
    fastq_1_url = dcp_utils.get_file_url(manifest_files, fastq_1_name)
    fastq_2_url = dcp_utils.get_file_url(manifest_files, fastq_2_name)

    print("Creating input map")
    with open("inputs.tsv", "w") as f:
        f.write("fastq_1\tfastq_2\tsample_id\n")
        f.write("{0}\t{1}\t{2}\n".format(fastq_1_url, fastq_2_url, sample_id))
    print("Wrote input map")


def create_optimus_input_tsv(uuid, version, dss_url, retry_seconds, timeout_seconds):
    """Create tsv of Optimus inputs"""
    # Get bundle manifest
    print('Getting bundle manifest for id {0}, version {1}'.format(uuid, version))
    manifest = dcp_utils.get_manifest(uuid, version, dss_url, timeout_seconds, retry_seconds)
    manifest_files = dcp_utils.get_manifest_file_dicts(manifest)

    inputs_metadata_json, sample_id_file_json, schema_version = get_metadata_to_process(
        manifest_files, dss_url, is_v5_or_higher(manifest_files['name_to_meta']))

    # Parse inputs from metadata and write to fastq_inputs
    print('Writing fastq inputs to fastq_inputs.tsv')
    sample_id = get_sample_id(sample_id_file_json, schema_version)
    lanes = get_optimus_lanes(input_metadata_json, schema_version)
    r1, r2, i1 = get_optimus_inputs(lanes, manifest_files)
    fastq_inputs = [list(i) for i in zip(r1, r2, i1)]
    print(fastq_inputs)

    with open('fastq_inputs.tsv', 'w') as f:
        for line in fastq_inputs:
            f.write('\t'.join(line) +'\n')
        print('Writing sample ID to inputs.tsv')
        f.write('{0}'.format(sample_id))

    print('Wrote input map')
