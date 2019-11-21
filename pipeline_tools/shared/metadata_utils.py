from humancellatlas.data.metadata.api import (
    Bundle,
    CellSuspension,
    LibraryPreparationProtocol,
)
from humancellatlas.data.metadata.helpers.dss import (
    download_bundle_metadata,
    dss_client,
)
from pipeline_tools.shared.exceptions import (
    UnsupportedOrganismException,
    UnsupportedLibraryPrepException,
)


def get_bundle_metadata(uuid, version, dss_url, directurls=False):
    """Factory function to create a `humancellatlas.data.metadata.Bundle` object from bundle information and manifest.

    Args:
        bundle_uuid (str): The bundle uuid.
        bundle_version (str): The bundle version.
        dss_url (str): Url of Data Storage System to query

    Returns:
        humancellatlas.data.metadata.Bundle: A bundle metadata object.
    """
    dss_deployment = dss_url.split('.')[1]
    if dss_deployment not in ('dev', 'integration', 'staging'):
        # dss_client constructor defaults to the production deployment
        client = dss_client()
    else:
        client = dss_client(deployment=dss_deployment)
    version, manifest, metadata_files = download_bundle_metadata(
        client=client, replica='gcp', uuid=uuid, version=version, directurls=directurls
    )
    return Bundle(
        uuid=uuid, version=version, manifest=manifest, metadata_files=metadata_files
    )


def get_sample_id(bundle):
    """Return the sample id from the given bundle.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        sample_id (str): String giving the sample id
    """
    sample_id = str(bundle.sequencing_input[0].document_id)
    return sample_id


def get_ncbi_taxon_id(bundle: Bundle):
    """Returns the ncbi_taxon_id for the Bundle, which identifies the sample organism

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        ncbi_taxon_id (int): integer value of the ncbi_taxon_id
    """
    cellSuspensions = [
        cs for cs in bundle.biomaterials.values() if isinstance(cs, CellSuspension)
    ]
    if len(cellSuspensions) != 1:
        raise UnsupportedOrganismException(
            'Multiple cell suspensions detected in bundle.'
        )
    cellSuspension = cellSuspensions[0]
    first_taxon_id = cellSuspension.ncbi_taxon_id[0]
    if any([taxon_id != first_taxon_id for taxon_id in cellSuspension.ncbi_taxon_id]):
        raise UnsupportedOrganismException(
            'Multiple distinct species detected in bundle.'
        )
    return first_taxon_id


def get_library_construction_method_ontology(bundle):
    """ Return the library construction method ontology id from the given bundle.

    Args:
        bundle (humancellatlas.data.metadata.Bundle): A Bundle object contains all of the necessary information.

    Returns:
        library_construction_method (str): ontology id of the library construction method (e.g. "EFO:0009310")

    Raises:
        UnsupportedLibraryPrepException: when the bundle contains more than or less than one library preparation protocol file
    """
    library_prep_protocols = [
        lp
        for lp in bundle.protocols.values()
        if isinstance(lp, LibraryPreparationProtocol)
    ]
    if len(library_prep_protocols) != 1:
        raise UnsupportedLibraryPrepException(
            'Multiple library preparation protocols detected in bundle.'
        )
    library_prep_protocol = library_prep_protocols[0]
    return library_prep_protocol.json['library_construction_method']['ontology']


def get_hashes_from_file_manifest(file_manifest):
    """ Return a string that is a concatenation of the file hashes provided in the bundle manifest entry for a file:
        {sha1}{sha256}{s3_etag}{crc32c}
    """
    sha1 = file_manifest.sha1
    sha256 = file_manifest.sha256
    s3_etag = file_manifest.s3_etag
    crc32c = file_manifest.crc32c
    return f'{sha1}{sha256}{s3_etag}{crc32c}'
