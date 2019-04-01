# pipeline-tools

[![Travis (.org) branch](https://img.shields.io/travis/HumanCellAtlas/pipeline-tools/master.svg?label=Unit%20Test%20on%20Travis%20CI%20&style=flat-square&logo=Travis)](https://travis-ci.org/HumanCellAtlas/pipeline-tools)
[![Read the Docs (version)](https://img.shields.io/readthedocs/pipeline-tools/stable.svg?label=ReadtheDocs%3A%20Latest&logo=Read%20the%20Docs&style=flat-square)](http://pipeline-tools.readthedocs.io/en/latest/?badge=latest)
[![GitHub release](https://img.shields.io/github/release/HumanCellAtlas/pipeline-tools.svg?label=Latest%20Release&style=flat-square&colorB=green)](https://github.com/HumanCellAtlas/pipeline-tools/releases)
[![Snyk Vulnerabilities for GitHub Repo (Specific Manifest)](https://img.shields.io/snyk/vulnerabilities/github/HumanCellAtlas/pipeline-tools/requirements.txt.svg?label=Snyk%20Vulnerabilities&logo=Snyk)](https://snyk.io/test/github/HumanCellAtlas/pipeline-tools?targetFile=requirements.txt)
[![Snyk Vulnerabilities for GitHub Repo (Specific Manifest)](https://img.shields.io/snyk/vulnerabilities/github/HumanCellAtlas/pipeline-tools/test-requirements.txt.svg?label=Snyk%20Test%20Requirment%20Vulnerabilities&logo=Snyk)](https://snyk.io/test/github/HumanCellAtlas/pipeline-tools?targetFile=test-requirements.txt)

![Github](https://img.shields.io/badge/python-3.6-green.svg?style=flat-square&logo=python&colorB=blue)
![GitHub](https://img.shields.io/github/license/HumanCellAtlas/pipeline-tools.svg?style=flat-square&colorB=blue)
[![Code style: black](https://img.shields.io/badge/Code%20Style-black-000000.svg?style=flat-square)](https://github.com/ambv/black)


This repository contains Python code and pipelines for interacting with the Human Cell Atlas Data Coordination Platform (DCP). They are used by the Secondary Analysis Service.

The pipelines wrap analysis pipelines from the Skylab repository and provide some glue to interface with the DCP. The adapter pipelines take bundle ids as inputs, query the Data Storage Service to find the input files needed by the analysis pipelines, then run the analysis pipelines and submit the results to the Ingest Service. This helps us keep the analysis pipelines themselves free of dependencies on the DCP.

Note: The adapter pipelines can only run in Cromwell instances that use SAM for Identity and Access Management (IAM), such as Cromwell-as-a-Service.

## Development

### Code Style

The Pipeline-tools code base is complying with the PEP-8 and using [Black](https://github.com/ambv/black) to 
format our code, in order to avoid "nitpicky" comments during the code review process so we spend more time discussing about the logic, not code styles.

In order to enable the auto-formatting in the development process, you have to spend a few seconds setting up the `pre-commit` the first time you clone the repo:

1. Install `pre-commit` by running: `pip install pre-commit` (or simply run `pip install -r requirements.txt`).
2. Run `pre-commit install` to install the git hook.

Please make sure you followed the above steps, otherwise your commits might fail at the linting test!

## Run tests

### Create a virtual environment

```
virtualenv pipeline-tools-test-env
source pipeline-tools-test-env/bin/activate
pip install -r test-requirements.txt
```

### Run unit tests

```
pytest -v
```
