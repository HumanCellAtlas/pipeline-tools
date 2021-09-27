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
        pipeline_type,
        file_name_string,
        workspace_version,
        output_file_path,
        input_uuids=[],
        analysis_process_path="",
        analysis_protocol_path="",
        input_uuids_path="",
        analysis_process_list_path="",
        analysis_protocol_list_path="",
        ss2_bam="",
        ss2_bai="",
        ss2_fastq1="",
        ss2_fastq2="",
            project_level=False):

        print(input_uuids)
        print(pipeline_type)

        # Create UUID to save the file as
        file_prehash = f"{file_name_string}"
        subgraph_uuid = format_map.get_uuid5(file_prehash)

        # Load outputs.json into memory
        # For SS2 links this will be the outputs from the project-level run
        with open(output_file_path) as f:
            outputs_dict = json.load(f)

        self.link_type = "process_link"
        self.process_type = "analysis_process"
        self.outputs = outputs_dict
        self.project_id = project_id
        self.pipeline_type = pipeline_type
        self.project_level = project_level
        self.subgraph_uuid = subgraph_uuid
        self.file_name_string = file_name_string
        self.workspace_version = workspace_version
        self.analysis_process_path = analysis_process_path
        self.analysis_protocol_path = analysis_protocol_path

        # If pipelinetype is optimus then input uuids come from a list
        # If pipeline type is SS2 then read the list from a file
        if pipeline_type.lower() == "optimus":
            print("pipeline is optimus")
            self.input_uuids = input_uuids
        else:
            with open(input_uuids_path) as f:
                self.input_uuids = json.load(f)

        # If the pipeline type is SS2 then we need to get all of these values from files of arrays
        # SS2 has runs over 10k sample and the arrays are too large to pass as parameters
        if pipeline_type.lower() == "ss2":

            with open(ss2_bam) as f:
                self.ss2_bam = json.load(f)

            with open(ss2_bai) as f:
                self.ss2_bai = json.load(f)

            with open(ss2_fastq1) as f:
                self.ss2_fastq1 = json.load(f)

            with open(analysis_process_list_path) as f:
                self.analysis_process_list_path = json.load(f)

            with open(analysis_protocol_list_path) as f:
                self.analysis_protocol_list_path = json.load(f)

            # If single end read then this will load an empty array
            with open(ss2_fastq2) as f:
                self.ss2_fastq2 = json.load(f)

    def __links_file_optimus__(self):
        """Links file json for Optimus, will contain only a single 'links' object in the list"""

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

    def __links_file_ss2__(self):
        """Links file json for SS2, will contain multiple 'links' object in the list"""

        return {
            "describedBy" : self.describedBy,
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version,
            "links" : self.__ss2_links__()
        }

    def __ss2_links__(self):
        """Create the list of links object for a run of SS2
        First gets all links for intermediate runs
        Finally appends the project level link

        Returns:
            links (array[object]) : List of links object for SS2 run"""

        links = []

        for index in range(len(self.input_uuids)):
            links_element = {
                "process_type": self.process_type,
                "link_type": self.link_type,
                "process_id": self.__process_id__(index),
                "inputs": self.__ss2_intermediate_inputs__(index),
                "outputs": self.__ss2_intermediate_outputs__(index),
                "protocols": self.__protocols__(index)
            }
            links.append(links_element)

        # Build the project link of all the intermediate bams and output loom
        project_link = self.__ss2_project_link__()

        links.append(project_link)

        return links

    def __ss2_project_link__(self):
        """Gets the project level link for an SS2 run where the inputs are the intermediate bam/bai files and output is project loom"""

        bam_hashes, bai_hashes = self.__hashes__()

        bam_inputs = list(map(lambda x : {
            "input_id" : x,
            "input_type" : "analysis_file"
        }, bam_hashes))

        bai_inputs = list(map(lambda x : {
            "input_id" : x,
            "input_type" : "analysis_file"
        }, bai_hashes))

        return {
            "process_type" : self.process_type,
            "link_type" : self.link_type,
            "process_id": self.__process_id__(),
            "inputs" : [*bam_inputs, *bai_inputs],
            "outputs" : self.__outputs__(),
            "protocols" : self.__protocols__()
        }

    def __optimus_inputs__(self):
        """Add all input files based off the supplied UUIDs, Optimus input object are non-nested
            inputs for intermediate are the fastq hashes, inputs for project are intermediate loom hashes
        """

        print(f'input-{self.input_uuids}')

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
            index(int): Index of the bam and bai hash to retrieve

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

    def __protocols__(self, index=""):
        """Gets the protocol ID from the single path provided for Optimus and SS2 project level
        Gets the protocol ID from the list of paths for SS2 intermediate

        Args:
            index (int): Index to grab the protocol id from list

        Returns:
            protocol (array[object]): Single item array containing the protocol_type and protocol_id"""

        return [
            {
                "protocol_type" : "analysis_protocol",
                "protocol_id" : re.findall(self.uuid_regex, self.analysis_protocol_path)[-1] if index == ""
                else re.findall(self.uuid_regex, self.analysis_protocol_list_path[index])[-1]
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

        return re.findall(self.uuid_regex, self.analysis_process_list_path[index])[-1]

    def get_json(self):

        if self.pipeline_type.lower() == "optimus":
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
    parser.add_argument('--input_uuids', required=False, nargs='+', help='List of UUIDs for the Optimus inputs (fastq for intermediate runs, intermediate looms for project runs)')
    parser.add_argument('--input_uuids_path', required=False, help='Localized path to the list of input uuids for SS2')
    parser.add_argument('--analysis_process_path', required=True, help='Path to the /metadata/analysis_process.json file for Optimus (both levels) and SS2 project-level')
    parser.add_argument('--analysis_protocol_path', required=True, help='Path to the /metadata/analysis_protocol.json file for Optimus (both levels) and SS2 project-level')
    parser.add_argument('--analysis_process_list_path', required=False, help='Localized path to the list of analysis process files /metadata/analysis_process.json for SS2 Intermediate')
    parser.add_argument('--analysis_protocol_list_path', required=False, help='Localized path to the list of analysis protocol files /metadata/analysis_protocol.json file for SS2 Intermediate, this will be a single value')
    parser.add_argument("--project_level", required=True, type=lambda x: bool(strtobool(x)), help="Boolean representing project level vs intermediate level")
    parser.add_argument('--workspace_version', required=True, help='A version (or timestamp) attribute shared across all workflows within an individual workspace.')
    parser.add_argument('--output_file_path', required=True, help='Path to the outputs.json file for Optimus, path to project level loom for ss2')
    parser.add_argument('--file_name_string', required=True, help='Input ID (a unique input ID to incorporate into the links UUID) OR project stratum string (concatenation of the project, library, species, and organ).')
    parser.add_argument('--ss2_bam', required=False, help="Localized path to array of bam files for the ss2 runs, used to build the file hashes")
    parser.add_argument('--ss2_bai', required=False, help="Localized path to array of bai files for the ss2 runs, used to build the file hashes")
    parser.add_argument('--ss2_fastq1', required=False, help="Localized path to array of fastq1 UUIDS for ss2 runs")
    parser.add_argument('--ss2_fastq2', required=False, help="Localized path to array of fastq2 UUIDSfor ss2 runs")

    args = parser.parse_args()

    links_file = LinksFile(
        args.project_id,
        args.pipeline_type,
        args.file_name_string,
        args.workspace_version,
        args.output_file_path,
        args.input_uuids,
        args.analysis_process_path,
        args.analysis_protocol_path,
        args.input_uuids_path,
        args.analysis_process_list_path,
        args.analysis_protocol_list_path,
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
