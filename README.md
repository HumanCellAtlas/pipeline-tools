# pipeline-tools

This repository contains Python package for interacting with and running data processing pipelines in the Human Cell Atlas Data Coordination Platform (DCP). 
They are used by the DCP Pipelines Execution Service (Secondary Analysis).

#### NOTE

**This repo should be considered 'legacy', code pertaining to the HCA adapters has been moved to [https://github.com/broadinstitute/hca-adapter-tools](https://github.com/broadinstitute/hca-adapter-tools)**


## Development

### Code Style

The Pipeline-tools code base is complying with the PEP-8 and using [Black](https://github.com/ambv/black) to 
format our code, in order to avoid "nitpicky" comments during the code review process so we spend more time discussing about the logic, not code styles.

In order to enable the auto-formatting in the development process, you have to spend a few seconds setting up the `pre-commit` the first time you clone the repo. It's highly recommended that you install the packages within a [`virtualenv`](https://virtualenv.pypa.io/en/latest/userguide/).

1. Install `pre-commit` by running: `pip install pre-commit` (or simply run `pip install -r requirements.txt`).
2. Run `pre-commit install` to install the git hook.

Once you successfully install the `pre-commit` hook to this repo, the Black linter/formatter will be automatically triggered and run on this repo. Please make sure you followed the above steps, otherwise your commits might fail at the linting test!

_If you really want to manually trigger the linters and formatters on your code, make sure `Black` and `flake8` are installed in your Python environment and run `flake8 DIR1 DIR2` and `black DIR1 DIR2 --skip-string-normalization` respectively._

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
