from pipeline_tools.shared import dcp_utils
from humancellatlas.data.metadata.api import Bundle, DonorOrganism
from pipeline_tools.shared.http_requests import HttpRequests
import functools
from concurrent.futures import ThreadPoolExecutor


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
    manifest = dcp_utils.get_manifest(
        bundle_uuid=uuid,
        bundle_version=version,
        dss_url=dss_url,
        http_requests=http_requests,
    )['bundle']['files']

    metadata_files_dict = {f['name']: f for f in manifest if f['indexed']}
    metadata_files = get_metadata_files(
        metadata_files_dict=metadata_files_dict, dss_url=dss_url
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
    donorOrganisms = [b for b in bundle.biomaterials.values() if isinstance(b, DonorOrganism)]
    return donorOrganisms[0].ncbi_taxon_id


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
    return (
        file_name,
        dcp_utils.get_file_by_uuid(
            file_id=file_uuid, dss_url=dss_url, http_requests=http_requests
        ),
    )


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
            map(
                functools.partial(
                    download_file, dss_url=dss_url, http_requests=HttpRequests()
                ),
                metadata_files_dict.items(),
            )
        )
    else:
        with ThreadPoolExecutor(num_workers) as tpe:
            metadata_files = dict(
                tpe.map(
                    functools.partial(
                        download_file, dss_url=dss_url, http_requests=HttpRequests()
                    ),
                    metadata_files_dict.items(),
                )
            )
    return metadata_files
