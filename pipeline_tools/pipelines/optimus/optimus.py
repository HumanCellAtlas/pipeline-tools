from pipeline_tools.shared import metadata_utils
from pipeline_tools.shared import tenx_utils
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.shared.reference_id import ReferenceId


REFERENCES = {
    ReferenceId.Human.value: {
        'tar_star_reference': 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/buildReference/output_bucket/star_primary_gencode_v27.tar',
        'annotations_gtf': 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/gencode.v27.primary_assembly.annotation.gtf.gz',
        'ref_genome_fasta': 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/GRCh38.primary_assembly.genome.fa',
    },
    ReferenceId.Mouse.value: {
        'tar_star_reference': 'gs://hca-dcp-mint-test-data/20190507-PipelinesSurge/mouse_reference/star_primary_gencode_mouse_vM21.tar',
        'annotations_gtf': 'gs://hca-dcp-mint-test-data/yanc-test/gencode.vM21.annotation.gtf.gz',
        'ref_genome_fasta': 'gs://hca-dcp-mint-test-data/yanc-test/GRCm38.primary_assembly.genome.fa',
    },
}


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
        tenx_utils.LaneMissingFileError if any non-optional fastqs are missing
    """
    # Get bundle manifest
    print(f"Getting bundle manifest for id {uuid}, version {version}")
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=uuid, version=version, dss_url=dss_url, http_requests=HttpRequests()
    )

    # Parse inputs from metadata
    print('Gathering fastq inputs')
    fastq_files = [
        f
        for f in primary_bundle.files.values()
        if f.file_format in ('fastq.gz', 'fastq')
    ]
    lane_to_fastqs = tenx_utils.create_fastq_dict(fastq_files)

    # Stop if any fastqs are missing
    tenx_utils.validate_lanes(lane_to_fastqs)

    r1_urls = tenx_utils.get_fastqs_for_read_index(lane_to_fastqs, 'read1')
    r2_urls = tenx_utils.get_fastqs_for_read_index(lane_to_fastqs, 'read2')
    i1_urls = tenx_utils.get_fastqs_for_read_index(lane_to_fastqs, 'index1')

    print('Writing r1.txt, r2.txt, and optionally i1.txt')
    with open('r1.txt', 'w') as f:
        for url in r1_urls:
            f.write(url + '\n')
    with open('r2.txt', 'w') as f:
        for url in r2_urls:
            f.write(url + '\n')

    # Always generate the i1.txt, if there are no i1 fastq files,
    # the content of this file will be empty
    with open('i1.txt', 'w') as f:
        for url in i1_urls:
            f.write(url + '\n')

    sample_id = metadata_utils.get_sample_id(primary_bundle)
    print('Writing sample ID to sample_id.txt')
    with open('sample_id.txt', 'w') as f:
        f.write(f"{sample_id}")

    species_references = REFERENCES[metadata_utils.get_ncbi_taxon_id(primary_bundle)]
    print('Writing species references')
    for key, value in species_references.items():
        with open(f"{key}.txt", 'w') as f:
            f.write(f"{value}")

    print('Finished writing files')
