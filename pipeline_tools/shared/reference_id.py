from enum import Enum


class ReferenceId(Enum):
    """
    This class maps one-to-one to ncbi taxon ids as found
    at https://www.ebi.ac.uk/ols/ontologies/ncbitaxon
    """

    Human = 9606
    Mouse = 10090
