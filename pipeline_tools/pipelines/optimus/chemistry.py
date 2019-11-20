from enum import Enum


class Chemistry(Enum):
    """
    Parameters for 10x chemistry used by the Optimus pipeline:
    https://github.com/HumanCellAtlas/skylab/blob/optimus_v1.4.0/pipelines/optimus/Optimus.wdl#L39
    """

    tenX_v2 = "tenX_v2"
    tenX_v3 = "tenX_v3"


class LibraryConstructionMethod(Enum):

    """
    This class maps to library construction ontology ids, e.g:
    https://www.ebi.ac.uk/ols/ontologies/efo/terms?iri=http%3A%2F%2Fwww.ebi.ac.uk%2Fefo%2FEFO_0008995&viewMode=All&siblings=false
    """

    tenX_v2 = "EFO:0009310"
    tenX_3_prime_v2 = "EFO:0009899"

    tenX_v3 = "EFO:0009898"
    tenX_3_prime_v3 = "EFO:0009922"
    tenX_5_prime_v3 = "EFO:0009921"
