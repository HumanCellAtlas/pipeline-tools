#!/usr/bin/env python
import argparse
import json
import os
import re


from pipeline_tools.shared.submission import format_map
from pipeline_tools.shared.schema_utils import SCHEMAS
from distutils.util import strtobool


class LinksFile():
    """LinksFile class implements the creation of a json file that contains the linked metadata for a given run of optimus or ss2

    The json file created should have the following form:

    {
        "describedBy": "https://schema.humancellatlas.org/system/2.1.1/links",
        "links": [
            {
                "inputs": [
                    {
                        "input_id": "heart_1k_test_v2_S1_L001_R1_001.fastq.gz",
                        "input_type": "sequence_file"
                    },
                    {
                        "input_id": "heart_1k_test_v2_S1_L001_R2_001.fastq.gz",
                        "input_type": "sequence_file"
                    }
                ],
                "link_type": "process_link",
                "outputs": [
                    {
                        "output_id": "87795ce9-03ce-51f3-b8d8-4ad6f8931fe0",
                        "output_type": "analysis_file"
                    },
                    {
                        "output_id": "d649938b-be5f-58cf-be04-0c1e6381eb9e",
                        "output_type": "analysis_file"
                    }
                ],
                "process_id": "151fe264-c670-4c77-a47c-530ff6b3127b",
                "process_type": "analysis_process",
                "protocols": [
                    {
                        "protocol_id": "f2cdb4e5-b439-5cdf-ac41-161ff39d5790",
                        "protocol_type": "analysis_protocol"
                    }
                ]
            }
        ],
        "schema_type": "links",
        "schema_version": "2.1.1"
    }

    See https://schema.humancellatlas.org/system/2.1.1/links for full spec
    """

    # All links  files will share these schema attributes
    describedBy = SCHEMAS["LINKS"]["describedBy"]
    schema_type = SCHEMAS["LINKS"]["schema_type"]
    schema_version = SCHEMAS["LINKS"]["schema_version"]

    # Regex to grab uuids from file paths
    uuid_regex = r"([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})"

    def __init__(
        self,
        project_id,
        input_uuids,
        pipeline_type,
        file_name_string,
        workspace_version,
        output_file_path,
        analysis_process_path="",
        analysis_protocol_path="",
        analysis_process_path_list=[],
        analysis_protocol_path_list="",
        ss2_bam=[],
        ss2_bai=[],
        ss2_fastq1=[],
        ss2_fastq2=[],
            project_level=False):

        # Create UUID to save the file as
        file_prehash = f"{file_name_string}"
        subgraph_uuid = format_map.get_uuid5(file_prehash)

        # Load outputs.json into memory
        # For SS2 links this will be the outputs from the project-level run
        with open(output_file_path) as f:
            outputs_dict = json.load(f)

        self.file_name_string = file_name_string
        self.outputs = outputs_dict
        self.project_id = project_id
        self.pipeline_type = pipeline_type
        self.input_uuids = input_uuids
        self.link_type = "process_link"
        self.project_level = project_level
        self.process_type = "analysis_process"
        self.subgraph_uuid = subgraph_uuid
        self.workspace_version = workspace_version
        self.analysis_protocol_path = analysis_protocol_path
        self.analysis_process_path = analysis_process_path

        print(analysis_protocol_path_list)
        print(analysis_process_path_list)

        # Specific to SS2
        self.ss2_bam = ss2_bam
        self.ss2_bai = ss2_bai
        self.ss2_fastq1 = ss2_fastq1
        self.ss2_fastq2 = ss2_fastq2
        self.analysis_process_path_list = analysis_process_path_list
        self.analysis_protocol_path_list = analysis_protocol_path_list

    # Optimus, whether project or intermediate level, only produces a single 'links' object
    def __links_file_optimus__(self):
        return {
            "describedBy" : self.describedBy,
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version,
            "links" : [
                {
                    "process_type" : self.process_type,
                    "link_type" : self.link_type,
                    "process_id" : self.__process_id__(),
                    "inputs" : self.__optimus_inputs__(),
                    "outputs" : self.__outputs__(),
                    "protocols" : self.__protocols__()
                }
            ]
        }

    # SS2 creates 'links' objects for every intermediate step of SS2 
    def __links_file_ss2__(self):
        return {
            "describedBy" : self.describedBy,
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version,
            "links" : self.__ss2_links__()
        }

    def __ss2_links__(self):

        links = []

        for index in range(len(self.input_uuids)):
            links_element = {
                "process_type": self.process_type,
                "link_type": self.link_type,
                "process_id": self.__process_id__(index),
                "inputs": self.__ss2_intermediate_inputs__(index),
                "outputs": self.__ss2_intermediate_outputs__(index),
                "protocols": self.__protocols__(0)
            }
            links.append(links_element)

        # Build the project link of all the intermediate bams and output loom
        project_link = self.__ss2_project_link__()

        links.append(project_link)

        return links

    def __ss2_project_link__(self):

        bam_hashes, _ = self.__hashes__()
        project_inputs = list(map(lambda x : {
            "input_id" : x,
            "input_type" : "analysis_file"
        }, bam_hashes))

        return {
            "process_type" : self.process_type,
            "link_type" : self.link_type,
            "process_id": self.__process_id__(),
            "inputs" : project_inputs,
            "outputs" : self.__outputs__(),
            "protocol" : self.__protocols__()
        }

    def __optimus_inputs__(self):
        """Add all input files based off the supplied UUIDs, Optimus input object are non-nested
            inputs for intermediate are the fastq hashes, inputs for project are intermediate loom hashes
        """

        inputs = []
        for input_uuid in self.input_uuids:
            inputs.append({'input_type': "analysis_file" if self.project_level else "sequence_file",
                          'input_id': input_uuid})
        return inputs

    def __ss2_intermediate_inputs__(self, index):
        """Builds the input list for an intermediate run of SS2
        Conditionally appends ss2_fastq if this is a paired end sample

        Args:
            index (int): Index of the ss2_fastq array to pull from

        Returns:
            intermediate_input (array[obj]): Array of input files (sequence files) for the intermediate SS2 run"""

        intermediate_input = [
            {
                "input_id" : self.ss2_fastq1[index],
                "input_type" : "sequence_file"
            }
        ]

        if self.ss2_fastq2:
            intermediate_input.append({
                "input_id" : self.ss2_fastq2[index],
                "input_type" : "sequence_file"
            })

        return intermediate_input

    def __ss2_intermediate_outputs__(self, index):
        """Build the output list for an intermediate run of SS2

        Args:
            index (int): Index of the bam and bai hash to retrieve

        Returns:
            intermediate_output (array[obj]): Array of output files (analysis files) for the intermediate SS2 run"""

        bam_hashes, bai_hashes = self.__hashes__()

        intermediate_output = [
            {
                "output_id": bam_hashes[index],
                "output_type": "analysis_file"
            },
            {
                "output_id": bai_hashes[index],
                "output_type": "analysis_file"
            }
        ]

        return intermediate_output

    def __hashes__(self):
        """For SS2 convert bam and bai file name to their correct hashes and return"""

        bam_hashes = []
        bai_hashes = []

        for index in range(len(self.input_uuids)):
            uuid = self.input_uuids[index]
            bam_file = self.ss2_bam[index]
            bai_file = self.ss2_bai[index]

            bam_hash = format_map.get_file_entity_id(uuid, format_map.get_entity_type(bam_file), os.path.splitext(bam_file)[1])
            bai_hash = format_map.get_file_entity_id(uuid, format_map.get_entity_type(bai_file), os.path.splitext(bai_file)[1])

            bam_hashes.append(bam_hash)
            bai_hashes.append(bai_hash)

        return bam_hashes, bai_hashes

    def __outputs__(self):
        """Add the outputs from outputs.json to an array and return
            This function is used for all optimus outputs and SS2 project outputs
        """

        outputs = []
        for output in self.outputs:
            output_type = output['describedBy'].split('/')[-1]
            output_id = output['provenance']['document_id']
            outputs.append({'output_type': output_type, 'output_id': output_id})

        return outputs

    def __protocols__(self, flag=""):
        """Gets the protocol ID from the single path provided for Optimus and SS2 project level
        Gets the protocol ID from the list of paths for SS2 intermediate

        Args:
            flag (int): Flag to determine to pull from protocol_path or protocol_path_list

        Returns:
            protocol (array[object]): Single item array containing the protocol_type and protocol_id"""

        return [
            {
                "protocol_type" : "analysis_protocol",
                "protocol_id" : re.findall(self.uuid_regex, self.analysis_protocol_path)[-1] if flag == ""
                else re.findall(self.uuid_regex, self.analysis_protocol_path_list)[-1]
            }
        ]

    def __process_id__(self, index=""):
        """Gets the process id from the single path provided for Optimus and SS2 project level
        Gets the process id from the list of paths for SS2 intermediate

        Args:
            index (int): Index of the process_path_list to grab the ID from

        Returns:
            process_id (string): process id in the form 151fe264-c670-4c77-a47c-530ff6b3127b"""

        if index == "" :
            return re.findall(self.uuid_regex, self.analysis_process_path)[-1]

        return re.findall(self.uuid_regex, self.analysis_process_path_list[index])[-1]

    def get_json(self):
        if self.pipeline_type.lower == "optimus":
            return self.__links_file_optimus__()

        return self.__links_file_ss2__()

    @property
    def version(self):
        return self.workspace_version

    @property
    def uuid(self):
        return self.subgraph_uuid

    @property
    def project(self):
        return self.project_id


# Entry point for unit tests
def test_build_links_file(
    project_id,
    input_uuids,
    output_file_path,
    file_name_string,
    workspace_version,
    analysis_process_path,
    analysis_protocol_path,
        project_level=False):

    test_links_file = LinksFile(
        project_id,
        input_uuids,
        output_file_path,
        file_name_string,
        workspace_version,
        analysis_process_path,
        analysis_protocol_path,
        project_level)

    return test_links_file.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True, help='The project ID')
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline(SS2 or Optimus)")
    parser.add_argument('--input_uuids', required=True, nargs='+', help='List of UUIDs for the input files Optimus(fastq for intermediate/looms for project) SS2 (uuids for each run to build the file hashes)')
    parser.add_argument('--analysis_process_path', required=False, help='Path to the /metadata/analysis_process.json file for Optimus (both levels) and SS2 project-level')
    parser.add_argument('--analysis_protocol_path', required=False, help='Path to the /metadata/analysis_protocol.json file for Optimus (both levels) and SS2 project-level')
    parser.add_argument('--analysis_process_path_list', nargs='*', required=False, help='List of paths for to the /metadata/analysis_process.json for SS2 Intermediate')
    parser.add_argument('--analysis_protocol_path_list', required=False, help='List to the path to the /metadata/analysis_protocol.json file for SS2 Intermediate, this will be a single value')
    parser.add_argument("--project_level", required=True, type=lambda x: bool(strtobool(x)), help="Boolean representing project level vs intermediate level")
    parser.add_argument('--workspace_version', required=True, help='A version (or timestamp) attribute shared across all workflows within an individual workspace.')
    parser.add_argument('--output_file_path', required=True, help='Path to the outputs.json file for Optimus, path to project level loom for ss2')
    parser.add_argument('--file_name_string', required=True, help='Input ID (a unique input ID to incorporate into the links UUID) OR project stratum string (concatenation of the project, library, species, and organ).')
    parser.add_argument('--ss2_bam', required=False, nargs='*', help="Array of bam files for the ss2 runs, used to build the file hashes")
    parser.add_argument('--ss2_bai', required=False, nargs='*', help="Array of bai files for the ss2 runs, used to build the file hashes")
    parser.add_argument('--ss2_fastq1', required=False, nargs='*', help="Array of fastq1 UUIDS for ss2 runs")
    parser.add_argument('--ss2_fastq2', required=False, nargs='*', help="Array of fastq2 UUIDSfor ss2 runs")

    args = parser.parse_args()

    links_file = LinksFile(
        args.project_id,
        args.input_uuids,
        args.pipeline_type,
        args.file_name_string,
        args.workspace_version,
        args.output_file_path,
        args.analysis_process_path,
        args.analysis_protocol_path,
        args.analysis_process_path_list,
        args.analysis_protocol_path_list,
        args.ss2_bam,
        args.ss2_bai,
        args.ss2_fastq1,
        args.ss2_fastq2,
        args.project_level
    )

    links_file_json = links_file.get_json()

    # Write links to file
    print("Writing links file to disk...")
    with open(f'{links_file.uuid}_{links_file.version}_{links_file.project}.json', 'w') as f:
        json.dump(links_file_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
