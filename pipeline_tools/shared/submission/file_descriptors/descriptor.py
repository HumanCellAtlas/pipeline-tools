import mimetypes

from operator import itemgetter
from pipeline_tools.shared.submission.format_map import MIME_FORMATS

class Descriptor():
    """Descriptor class implements the creation of a json file descriptor for Optimus and SS2 pipeline outputs
    
    HCA system consumes json files that describe the .bam and .loom outputs of Optimus and SS2 pipelines
    
    The json files have the following form:

        {
        "content_type": "application/vnd.loom",
        "crc32c": "e598a0f6",
        "describedBy": "https://schema.humancellatlas.org/system/2.0.0/file_descriptor",
        "file_id": "317a2bfc-ea58-50ae-b64e-4c58d0c01a74",
        "file_name": "heart_1k_test_v2_S1_L001.loom",
        "file_version": "2021-07-08T17:22:45.000000Z",
        "schema_type": "file_descriptor",
        "schema_version": "2.0.0",
        "sha256": "c12d50051a5b8820124f596529c6cbdc0280b71883acbde08e30311cdb30edfa",
        "size": 21806469
        }
        
    See https://schema.humancellatlas.org/system/2.0.0/file_descriptor for full spec
"""
    [mimetypes.add_type(entry[0], entry[1]) for entry in MIME_FORMATS]

    describedBy ="https://schema.humancellatlas.org/system/2.0.0/file_descriptor"
    schema_type="file_descriptor"
    schema_version="2.0.0"


    def __init__(self, size, sha256, crc32c, input_uuid, file_path, creation_time):
        self.size = size
        self.sha256 = sha256
        self.crc32c = crc32c
        self.input_uuid = input_uuid
        self.file_path = file_path
        self.creation_time = creation_time

    def __descriptor__(self):
        return {
            describedBy: self.describedBy,
            schema_version: self.schema_version,
            schema_type: self.file_descriptor
            
        }
    
    def dude(self):
        print(MIME_FORMATS)