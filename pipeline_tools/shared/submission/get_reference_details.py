import argparse
import os

HUMAN = "Homo Sapiens"
MOUSE = "Mus musculus"

def check_reference_and_species(reference_filename, species):
    if "grch" in reference_filename.lower() and species != HUMAN:
        raise UnknownReferenceError('Reference file must match the species. {} is not a known reference for {}.'.format(reference_file_path, species))
    elif ("grcm" in reference_filename.lower() or "mm10" in reference_filename.lower())and species != MOUSE:
        raise UnknownReferenceError('Reference file must match the species. {} is not a known reference for {}.'.format(reference_file_path, species))

def get_taxon_id(species):
    if species == HUMAN:
        return "9606"
    elif species == MOUSE:
        return "10090"
    else:
        raise UnknownReferenceError('Species must be either mouse ("Mus musculus") or human ("Homo sapiens")')

def get_assembly_type(reference_filename):
    """
    :param reference_filename:
    :return: the assembly type for the specified file

    This field is an enu with the following possiblle values:
    'primary assembly',
    'complete assembly',
    'patch assembly'
    """
    if "primary_assembly" in reference_filename:
        return "primary assembly"
    else:
        raise UnknownReferenceError('Reference with unknown "assembly type"')


def get_reference_type(reference_filename):
    """
    :param reference_filename:
    :return: the referecne type for the specified file

    This field is an enu with the following possiblle values:
    'genome sequence',
    'transcriptome sequence',
    'annotation reference',
    'transcriptome index',
    'genome sequence index'
    """
    if "genome" in reference_filename:
        return "genome sequeence"
    else:
        raise UnknownReferenceError('Reference with unknown "reference type"')



def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--reference-file',
                        dest='reference_file',
                        required=True,
                        help='path to cromwell metadata json')

    parser.add_argument('--species',
                        dest='species',
                        required=True,
                        help='species of input samples')

    args = parser.parse_args()

    reference_file = args.reference_file
    species = args.species

    reference_filename = os.path.basename(reference_file)

    check_reference_and_species(reference_filename, species)

    ncbi_taxon_id = get_taxon_id(species)
    assembly_type = get_assembly_type(reference_filename)
    reference_type = get_reference_type(reference_filename)


    with open('ncbi_taxon_id.txt', 'w') as f:
        f.write(ncbi_taxon_id)

    with open('assembly_type.txt', 'w') as f:
        f.write(assembly_type)

    with open('reference_type.txt', 'w') as f:
        f.write(reference_type)


if __name__ == '__main__':
    main()


class UnknownReferenceError(Exception):
    pass