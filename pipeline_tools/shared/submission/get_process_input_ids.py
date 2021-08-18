#!/usr/bin/env python3

import json
import argparse


def main():
    description = """Collects input ids from individual analysis file jsons """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--input-json-files',
                        dest='input_files',
                        nargs="+",
                        required=True,
                        help="List of json files")

    args = parser.parse_args()

    analysis_files = args.input_files

    inputs = []

    for analysis_file in analysis_files:
        with open(analysis_file, "r") as f:
            analysis_metadata = json.load(f)
        if analysis_metadata["file_core"]["file_name"].endswith(".loom"):
            input_uuid = analysis_metadata["provenance"]["document_id"]
            inputs.append(input_uuid)

    with open("output.txt", "w") as f:
        f.write(" ".join(inputs))


if __name__ == '__main__':
    main()