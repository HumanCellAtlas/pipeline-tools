import csv
import json

from pipeline_tools.shared import metadata_utils
from pipeline_tools.shared import tenx_utils
from pipeline_tools.shared.reference_id import ReferenceId
from pipeline_tools.pipelines.optimus.chemistry import (
    Chemistry,
    LibraryConstructionMethod,
)


REFERENCES = {
    ReferenceId.Human.value: {
        'tar_star_reference': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/buildReference/output_bucket/star_primary_gencode_v27.tar',
        'annotations_gtf': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/gencode.v27.primary_assembly.annotation.gtf.gz',
        'ref_genome_fasta': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/GRCh38.primary_assembly.genome.fa',
    },
    ReferenceId.Mouse.value: {
        'tar_star_reference': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencode_Mouse_M21/star_primary_gencode_mouse_vM21.tar',
        'annotations_gtf': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencode_Mouse_M21/gencode.vM21.annotation.gtf.gz',
        'ref_genome_fasta': 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencode_Mouse_M21/GRCm38.primary_assembly.genome.fa',
    },
}

LIBRARY_CONSTRUCTION_METHODS = {
    Chemistry.tenX_v2.value: [
        LibraryConstructionMethod.tenX_v2.value,
        LibraryConstructionMethod.tenX_3_prime_v2.value,
    ],
    Chemistry.tenX_v3.value: [
        LibraryConstructionMethod.tenX_v3.value,
        LibraryConstructionMethod.tenX_3_prime_v3.value,
        LibraryConstructionMethod.tenX_5_prime_v3.value,
    ],
}


def get_tenx_chemistry(library_construction_method_ontology):
    """
    Determine the tenX chemistry that was used based on the given library construction method. For example, if the
    library construction method is EFO:0009310 (corresponding to LibraryConstructionMethod.tenX_v2.value), then the
    chemistry is "tenX_v2".

    Args:
        library_construction_method_ontology (str): ontology id of the library construction method (e.g. "EFO:0009310")

    Returns:
        chemistry (str): The tenX chemistry (either tenxX_v2 or tenX_v3)

    """
    chemistry = None
    for each in LIBRARY_CONSTRUCTION_METHODS:
        if library_construction_method_ontology in LIBRARY_CONSTRUCTION_METHODS[each]:
            chemistry = each
    if not chemistry:
        raise tenx_utils.UnsupportedTenXChemistryError('Unsupported tenX chemistry')
    return chemistry


def get_optimus_inputs(primary_bundle):
    """Gather the necessary inputs for Optimus from the bundle metadata object.

    Args:
        humancellatlas.data.metadata.Bundle (obj): A bundle metadata object.

    Returns:
        tuple: tuple of the sample_id, ncbi_taxon_id, dict mapping flow cell lane indices
               to fastq file manifests

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout
    """
    sample_id = metadata_utils.get_sample_id(primary_bundle)
    ncbi_taxon_id = metadata_utils.get_ncbi_taxon_id(primary_bundle)
    fastq_files = primary_bundle.sequencing_output
    lane_to_fastqs = tenx_utils.create_fastq_dict(fastq_files)
    library_construction_method = metadata_utils.get_library_construction_method_ontology(
        primary_bundle
    )
    chemistry = get_tenx_chemistry(library_construction_method)
    return sample_id, ncbi_taxon_id, lane_to_fastqs, chemistry


def get_optimus_inputs_to_hash(uuid, version, dss_url):
    """
    Get values to hash for the worflow input Cromwell label, including the
    sample_id, ncbi_taxon_id and the file hashes for each fastq file in the data bundle
    grouped by lane index.

    Args:
        bundle_uuid (str): the bundle uuid.
        bundle_version (str): the bundle version.
        dss_url (str): the url for the DCP Data Storage Service.

    Returns:
        tuple: tuple of the sample_id, ncbi_taxon_id and fastq file hashes

    Raises:
        requests.HTTPError: on 4xx errors or 5xx errors beyond the timeout

    """
    print(f"Getting bundle manifest for id {uuid}, version {version}")
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=uuid, version=version, dss_url=dss_url, directurls=False
    )
    sample_id, ncbi_taxon_id, lane_to_fastqs, chemistry = get_optimus_inputs(
        primary_bundle
    )
    sorted_lanes = sorted(lane_to_fastqs.keys(), key=int)
    file_hashes = ''
    for lane in sorted_lanes:
        r1_hashes = metadata_utils.get_hashes_from_file_manifest(
            lane_to_fastqs[lane]['read1']
        )
        r2_hashes = metadata_utils.get_hashes_from_file_manifest(
            lane_to_fastqs[lane]['read2']
        )
        file_hashes += f'{r1_hashes}{r2_hashes}'
        if lane_to_fastqs[lane].get('index1'):
            i1_hashes = metadata_utils.get_hashes_from_file_manifest(
                lane_to_fastqs[lane]['index1']
            )
            file_hashes += i1_hashes
    # This order MUST be maintained to compare input hashes between different Optimus workflows!
    return sample_id, ncbi_taxon_id, file_hashes, chemistry


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
        tenx_utils.UnsupportedTenXChemistry if get_tenx_chemistry returns None
    """
    print(f"Getting bundle manifest for id {uuid}, version {version}")
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=uuid, version=version, dss_url=dss_url, directurls=True
    )
    sample_id, ncbi_taxon_id, lane_to_fastqs, chemistry = get_optimus_inputs(
        primary_bundle
    )

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

    print('Writing sample ID to sample_id.txt')
    with open('sample_id.txt', 'w') as f:
        f.write(f"{sample_id}")

    ref_id = ReferenceId(ncbi_taxon_id)
    species_references = REFERENCES[ref_id.value]
    print(f"Writing species references for {ref_id.name}")
    for key, value in species_references.items():
        print(f"Writing {key}.txt")
        with open(f"{key}.txt", 'w') as f:
            f.write(f"{value}")

    print(f'Detected {chemistry} chemistry and writing to chemistry.txt')
    with open('chemistry.txt', 'w') as f:
        f.write(f"{chemistry}")

    print('Finished writing files')


def create_optimus_inputs_tsv_from_analysis_metadata(metadata_file):
    """Create TSV of Optimus inputs from Cromwell metadata

    Args:
        metadata_file (str): path to the Cromwell metadata json from the analysis.

    Returns:
        TSV of input values for specific arguments
    """
    with open(metadata_file) as f:
        metadata = json.load(f)
    HEADERS = ['name', 'value']
    content = [
        ['r1_fastq', metadata['inputs']['r1_fastq']],
        ['r2_fastq', metadata['inputs']['r2_fastq']],
        ['i1_fastq', metadata['inputs']['i1_fastq']],
        ['whitelist', metadata['inputs']['whitelist']],
        ['input_id', metadata['inputs']['input_id']],
        ['tar_star_reference', metadata['inputs']['tar_star_reference']],
        ['annotations_gtf', metadata['inputs']['annotations_gtf']],
        ['ref_genome_fasta', metadata['inputs']['ref_genome_fasta']],
        ['chemistry', metadata['inputs']['chemistry']],
    ]

    with open('inputs.tsv', 'w') as inputs_tsv:
        writer = csv.writer(inputs_tsv, delimiter='\t')
        writer.writerow(HEADERS)
        writer.writerows(content)
