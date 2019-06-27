from pipeline_tools.shared import metadata_utils
from pipeline_tools.shared import tenx_utils
from pipeline_tools.shared.http_requests import HttpRequests
from humancellatlas.data.metadata.api import CellSuspension
from pipeline_tools.shared.reference_id import ReferenceId


REFERENCES = {
    ReferenceId.Human.value: {
        'reference_name': 'GRCh38',
        'transcriptome_tar_gz': 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/GRCh38_GencodeV27_Primary_CellRanger.tar',
    }
}


def create_cellranger_input_tsv(uuid, version, dss_url):
    """ Get inputs for cellranger count workflow

    Args:
        uuid (str): the bundle uuid
        version (str): the bundle version
        dss_url (str): the DCP Data Storage Service

    Returns:
        None

    Raises:
        tenx_utils.LaneMissingFileError: if any fastqs are missing
    """
    # Get bundle manifest
    print(f"Getting bundle manifest for id {uuid}, version {version}")
    primary_bundle = metadata_utils.get_bundle_metadata(
        uuid=uuid, version=version, dss_url=dss_url, http_requests=HttpRequests()
    )

    sample_id = metadata_utils.get_sample_id(primary_bundle)
    print('Writing sample ID to sample_id.txt')
    with open('sample_id.txt', 'w') as f:
        f.write(sample_id)

    total_estimated_cells = get_expected_cell_count(primary_bundle)
    print('Writing total estimated cells to expect_cells.txt')
    with open('expect_cells.txt', 'w') as f:
        f.write(str(total_estimated_cells))

    # Parse inputs from metadata
    print('Gathering fastq inputs')
    fastq_files = [
        f
        for f in primary_bundle.files.values()
        if str(f.format).lower() in ('fastq.gz', 'fastq')
    ]
    lane_to_fastqs = tenx_utils.create_fastq_dict(fastq_files)

    # Stop if any fastqs are missing
    tenx_utils.validate_lanes(lane_to_fastqs)

    read_indices = {'read1': 'R1', 'read2': 'R2', 'index1': 'I1'}
    fastq_urls = []
    fastq_names = []

    for lane, reads in lane_to_fastqs.items():
        for read_index, manifest_entry in reads.items():
            new_file_name = (
                f"{sample_id}_S1_L00{str(lane)}_{read_indices[read_index]}_001.fastq.gz"
            )
            fastq_names.append(new_file_name)
            fastq_urls.append(manifest_entry.url)

    with open('fastqs.txt', 'w') as f:
        for url in fastq_urls:
            f.write(url + '\n')

    with open('fastq_names.txt', 'w') as f:
        for name in fastq_names:
            f.write(name + '\n')

    ref_id = ReferenceId(metadata_utils.get_ncbi_taxon_id(primary_bundle))
    species_references = REFERENCES[ref_id.value]
    print(f"Writing species references for {ref_id.name}")
    for key, value in species_references.items():
        print(f"Writing {key}.txt")
        with open(f"{key}.txt", 'w') as f:
            f.write(f"{value}")

    print('Finished writing files')


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
    cell_suspension = [
        f for f in bundle.biomaterials.values() if isinstance(f, CellSuspension)
    ]
    n_cell_suspension = len(cell_suspension)
    if n_cell_suspension != 1:
        raise MoreThanOneCellSuspensionError(
            'The data bundle should contain exactly 1 cell_suspension.json file, '
            + f"not {n_cell_suspension}"
        )
    default_estimated_cells = 3000
    total_estimated_cells = cell_suspension[0].total_estimated_cells
    return (
        int(total_estimated_cells) if total_estimated_cells else default_estimated_cells
    )


class MoreThanOneCellSuspensionError(Exception):
    pass
