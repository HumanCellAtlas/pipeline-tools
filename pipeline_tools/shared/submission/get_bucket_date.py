import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--timestamp', dest='timestamp', required=True,
                        help='Timestamp from gs:// in Day, DD MONTH YYYY HH::MM:SS format')

    args = parser.parse_args()

    timestamp = args.timestamp

    # Have to convert timestampe to padded ISO8601
    formatted_date = datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S')
    formatted_date_padded = f"{formatted_date.isoformat()}.000000Z"

    with open('bucket_timestamp.txt', 'w') as f:
        f.write(formatted_date_padded)


if __name__ == '__main__':
    main()
