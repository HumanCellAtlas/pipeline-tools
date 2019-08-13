from pipeline_tools.shared import metadata_utils
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.shared.reference_id import ReferenceId


REFERENCES = {
    ReferenceId.Human.value: {
        'hisat2_ref_trans_name': 'gencode_v27_trans_rsem',
        'rrna_intervals': 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/gencode.v27.rRNA.interval_list',
        'hisat2_ref_index': 'gs://hca-dcp-mint-test-data/reference/HISAT2/genome_snp_tran.tar.gz',
        'genome_ref_fasta': 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/GRCh38.primary_assembly.genome.fa',
        'hisat2_ref_trans_index': 'gs://hca-dcp-mint-test-data/reference/HISAT2/gencode_v27_trans_rsem.tar.gz',
        'rsem_ref_index': 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/gencode_v27_primary.tar',
        'gene_ref_flat': 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/GRCh38_gencode.v27.refFlat.txt',
        'hisat2_ref_name': 'genome_snp_tran',
        'stranded': 'NONE',
    }
}


def get_ss2_paired_end_inputs(primary_bundle):
    """Gather the necessary inputs for ss2 from the bundle metadata object.

    Args:
        humancellatlas.data.metadata.Bundle (obj): A bundle metadata object.

    Returns:
        tuple: tuple of the sample_id, ncbi_taxon_id, fastq1_manifest object and fastq2_manifest object

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout
    """
    sample_id = metadata_utils.get_sample_id(primary_bundle)
    ncbi_taxon_id = metadata_utils.get_ncbi_taxon_id(primary_bundle)
    fastq1_manifest, fastq2_manifest = get_fastq_manifest_entry_for_ss2(primary_bundle)
    return sample_id, ncbi_taxon_id, fastq1_manifest, fastq2_manifest


def get_ss2_paired_end_inputs_to_hash(bundle_uuid, bundle_version, dss_url):
    print(
        "Getting bundle manifest for id {0}, version {1}".format(
            bundle_uuid, bundle_version
        )
    )
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=bundle_uuid,
        version=bundle_version,
        dss_url=dss_url,
        http_requests=HttpRequests(),
        directurls=False,
    )
    sample_id, ncbi_taxon_id, fastq1_manifest, fastq2_manifest = get_ss2_paired_end_inputs(
        primary_bundle
    )
    fastq1_hashes = metadata_utils.get_hashes_from_file_manifest(fastq1_manifest)
    fastq2_hashes = metadata_utils.get_hashes_from_file_manifest(fastq2_manifest)
    # This order MUST be maintained to compare input hashes between different SS2 workflows!
    return sample_id, ncbi_taxon_id, fastq1_hashes, fastq2_hashes


def create_ss2_input_tsv(
    bundle_uuid, bundle_version, dss_url, input_tsv_name='inputs.tsv'
):
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
    print(
        "Getting bundle manifest for id {0}, version {1}".format(
            bundle_uuid, bundle_version
        )
    )
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=bundle_uuid,
        version=bundle_version,
        dss_url=dss_url,
        http_requests=HttpRequests(),
        directurls=True,
    )
    sample_id, ncbi_taxon_id, fastq1_manifest, fastq2_manifest = get_ss2_paired_end_inputs(
        primary_bundle
    )
    tsv_headers = ['fastq_1', 'fastq_2', 'sample_id']
    tsv_values = [fastq1_manifest.url, fastq2_manifest.url, sample_id]

    species_references = REFERENCES[ncbi_taxon_id]
    for key, value in species_references.items():
        tsv_headers.append(key)
        tsv_values.append(value)

    print('Creating input map')
    with open(input_tsv_name, 'w') as f:
        f.write('\t'.join(tsv_headers) + '\n')
        f.write('\t'.join(tsv_values) + '\n')
    print('Wrote input map to disk.')


def get_fastq_manifest_entry_for_ss2(bundle):
    """Return the bundle manifest file entry for the input fastq files for SmartSeq2 pipeline.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
         tuple: A tuple consisting of the bundle manifest file entry for the input fastq files, which are corresponding to
         read index 1 and 2 respectively.
    """
    fastq1_manifest_entry = fastq2_manifest_entry = None
    sequence_files = bundle.sequencing_output

    for sf in sequence_files:
        if fastq1_manifest_entry and fastq2_manifest_entry:
            return fastq1_manifest_entry, fastq2_manifest_entry  # early termination
        if sf.read_index == 'read1':
            fastq1_manifest_entry = sf.manifest_entry
        if sf.read_index == 'read2':
            fastq2_manifest_entry = sf.manifest_entry
    return fastq1_manifest_entry, fastq2_manifest_entry


def create_ss2_se_input_tsv(
    bundle_uuid, bundle_version, dss_url, input_tsv_name='inputs.tsv'
):
    """Create TSV of Smart-seq2-SingleEnd inputs.

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
    fastq_url, sample_id = _get_content_for_ss2_se_input_tsv(
        bundle_uuid, bundle_version, dss_url, HttpRequests()
    )

    print('Creating input map')
    with open(input_tsv_name, 'w') as f:
        f.write('fastq\tsample_id\n')
        f.write('{0}\t{1}\n'.format(fastq_url, sample_id))
    print('Wrote input map to disk.')


def get_urls_to_files_for_ss2_se(bundle):
    """Return the direct urls to the input fastq files for SmartSeq2 pipeline.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
         tuple: A tuple consisting of the url to the input fastq file
         respectively.
    """

    sequence_files = bundle.sequencing_output

    for sf in sequence_files:
        # In single-end reads, the one fastq is called read1
        if sf.read_index == 'read1':
            return sf.manifest_entry.url


def _get_content_for_ss2_se_input_tsv(
    bundle_uuid, bundle_version, dss_url, http_requests
):
    """Gather the necessary metadata for the ss2 input tsv.

    Args:
        bundle_uuid (str): the bundle uuid.
        bundle_version (str): the bundle version.
        dss_url (str): the url for the DCP Data Storage Service.
        http_requests (HttpRequests): the HttpRequests object to use.

    Returns:
        tuple: tuple of two strings; url for the fastq, sample id

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout
    """

    print(
        "Getting bundle manifest for id {0}, version {1}".format(
            bundle_uuid, bundle_version
        )
    )
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=bundle_uuid,
        version=bundle_version,
        dss_url=dss_url,
        http_requests=http_requests,
    )

    sample_id = metadata_utils.get_sample_id(primary_bundle)
    fastq_url = get_urls_to_files_for_ss2_se(primary_bundle)
    return fastq_url, sample_id
