import argparse


def get_file_name_from_path(file_path):
    return file_path.split('/')[-1]


def get_files_to_upload(files, uploaded_files):
    files_to_upload = []
    for f in files:
        file_name = get_file_name_from_path(f)
        if file_name not in uploaded_files:
            files_to_upload.append(f)
    return files_to_upload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--files', nargs='+', required=True)
    parser.add_argument('--uploaded_files', nargs='+', required=True)
    args = parser.parse_args()

    # Get files that are not in the list of already uploaded files
    files = get_files_to_upload(args.files, args.uploaded_files)
    with open('files.txt', 'w') as output_file:
        for each in files:
            output_file.write(each + '\n')


if __name__ == '__main__':
    main()
