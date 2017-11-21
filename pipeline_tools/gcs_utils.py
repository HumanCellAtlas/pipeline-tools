"""This module contains utility functions and classes to interact with Google Cloud Storage Service.
"""
import logging
from google.cloud import storage
import google.auth
from io import BytesIO


def get_filename_from_gs_link(link):
    """Get the filename corresponding to a google_storage link.

    :param str link: A string of google cloud storage link.
    :return str: A string of filename.
    """
    return link.split('/')[-1]


def parse_bucket_blob_from_gs_link(path):
    """Utility to split a google storage path into bucket + blob name.

    :param str path: A string of google cloud storage path (must have gs:// prefix)
    :return str: A string of bucket name.
    :return str: A string of blob name
    """
    if not path.startswith('gs://'):
        raise ValueError('%s path is not a valid link')
    parts = path.split('/', 3)
    (prefix, _, bucket), blob = parts[:3], parts[3]

    return bucket, blob


def download_to_buffer(blob):
    """Return a bytes file-like object readable by requests and REST APIs

    :param google.cloud.storage.Blob blob: google storage blob
    :return _io.BufferedIOBase: readable file object
    """
    bytes_buffer = BytesIO()
    blob.download_to_file(bytes_buffer)
    bytes_buffer.seek(0)
    return bytes_buffer


def download_gcs_blob(gcs_client, bucket_name, source_blob_name):
    """Use google.cloud.storage API to download a blob from the bucket.

    :param GoogleCloudStorageClient gcs_client: A GoogleCloudStorageClient object with a
            google.cloud.storage.client.Client instance as a lazy-initialized property.
    :param str bucket_name: A string of bucket name.
    :param str source_blob_name: A string of source blob name that to be downloaded.
    :return BufferedIOBase: File-like object returned by download_to_buffer.
    """
    logging.getLogger()

    # Make sure the lazy property storage_client of gcs_client instance initialized at least once
    if not gcs_client.storage_client:
        gcs_client.storage_client
    authenticated_gcs_client = gcs_client.storage_client

    bucket = authenticated_gcs_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    logging.debug('bucket: {0}, blob: {1}'.format(bucket_name, source_blob_name))
    return download_to_buffer(blob)


class LazyProperty(object):
    """This class implements a decorator for lazy-initializing class properties.

        Instead of implementing Singleton Pattern, this decorator accepts multiple
        instances of a class, meanwhile, implements lazy initialization of certain
        decorated property. That specific read-only property only gets initialized
        on access, but once accessed, it would be cached and not re-initialized on
        each access.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            val = self.func(instance)
            setattr(instance, self.func.__name__, val)
            return val


class GoogleCloudStorageClient(object):
    def __init__(self, key_location, scopes):
        """This class implements the client to interact with Google Cloud Storage.

        :param str key_location: The location of Google Cloud Storage API key.
        :param list scopes: A list of OAuth 2.0 scopes information.
        """
        self.key_location, self.scopes = key_location, scopes

    @LazyProperty
    def storage_client(self):
        """This lazy property returns an authenticated google cloud storage client.

        :return google.cloud.storage.client.Client: An authenticated google cloud storage client.
        """
        logging.getLogger()
        logging.debug('Configuring listener credentials using %s' % self.key_location)

        credentials, project = google.auth.default(scopes=self.scopes)
        client = storage.Client(credentials=credentials, project=project)
        return client
