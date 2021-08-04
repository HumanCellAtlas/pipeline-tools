import argparse
import os

HUMAN = "Homo Sapiens"
MOUSE = "Mus musculus"

def check_reference_and_species(reference_file_path, species):
    refernce_filename = os.path.basename(reference_file_path)
    if "grch" in refernce_filename.lower() and species != HUMAN:
        raise UnknownReferenceError('Reference file must match the species. {} is not a known reference for {}.'.format(reference_file_path, species))
    elif ("grcm" in refernce_filename.lower() or "mm10" in refernce_filename.lower())and species != MOUSE:
        raise UnknownReferenceError('Reference file must match the species. {} is not a known reference for {}.'.format(reference_file_path, species))

def get_taxon_id(species):
    if species == HUMAN:
        return "9606"
    elif species == MOUSE:
        return "10090"
    else:
        raise UnknownReferenceError('Species must be either mouse ("Mus musculus") or human ("Homo sapiens")')

def get_assembly_type(reference_file):
    return "primary assembly"


def get_reference_type(reference_file):
    return "genome sequeence"


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

    check_reference_and_species(reference_file, species)

    ncbi_taxon_id = get_taxon_id(species)

    assembly_type = get_assembly_type(reference_file)

    reference_type = get_reference_type(reference_file)


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