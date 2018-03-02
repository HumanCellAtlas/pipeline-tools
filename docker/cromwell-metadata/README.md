This docker image is used in the `adapter_pipelines/submit.wdl` when retrieving metadata for the analysis pipeline from 
Cromwell. Depending on the instance of Cromwell being used, the request to retrieve metadata may require a username and password, or
a service account JSON key for Cromwell-as-a-Service (CAAS).

Prior to building the docker image:

1. Add the service account JSON key file for CAAS as `caas_key.json`. If you are not running the adapter 
pipelines in CAAS, this file can remain empty.
2. Add a `cromwell_credentials.txt` file that follows the format in `cromwell_credentials_template.txt`. If you are using 
CAAS, this file can remain empty.