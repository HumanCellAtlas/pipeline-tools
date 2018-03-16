from pipeline_tools import dcp_utils


def get_sample_id(metadata, version='4'):
    """Return the sample id from the given metadata"""
    if version == '4':
        return _get_sample_id_v4(metadata)
    else:
        raise NotImplementedError('Only implemented for v4 metadata')


def _get_sample_id_v4(assay_json):
    """Return sample id from assay json"""
    return assay_json["has_input"]


def get_input_metadata_file_uuid(manifest_files, version='4'):
    """Get the uuid of the file containing metadata about pipeline input files,
    e.g. assay.json in v4"""
    if version == '5':
        return _get_input_metadata_file_uuid_v5(manifest_files)
    elif version == '4':
        return _get_input_metadata_file_uuid_v4(manifest_files)
    else:
        raise NotImplementedError('Only implemented for v4 and v5 metadata')


def _get_input_metadata_file_uuid_v5(manifest_files):
    """Get the uuid of the files.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'files.json')


def _get_input_metadata_file_uuid_v4(manifest_files):
    """Get the uuid of the assay.json file"""
    return dcp_utils.get_file_uuid(manifest_files, 'assay.json')


def get_smart_seq_2_fastq_names(metadata, version='4'):
    """Get the fastq file names from the given metadata"""
    if version == '5':
        return _get_smart_seq_2_fastq_names_v5(metadata)
    elif version == '4':
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


def get_optimus_lanes(metadata_json, version='4'):
    """Get the lane metadata"""
    if version == '4':
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
