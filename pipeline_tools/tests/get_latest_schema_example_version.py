import argparse
import os

class Version:
    def __init__(self, dirname):
        self.dirname = dirname
        # Remove 'v' prefix
        d = dirname[1:]
        parts = d.strip().split('.')
        numeric_parts = list(map(lambda x: int(x), parts))
        self.v = numeric_parts
        while len(self.v) < 3:  
            self.v.append(0)
        self.string = '{0}.{1}.{2}'.format(self.v[0], self.v[1], self.v[2])

    def get_dirname(self):
        return self.dirname

    def __str__(self):
        return self.string

    def __eq__(self, other):
        return self.string == other.string

    def __gt__(self, other):
        for s, o in zip(self.v, other.v):
            if s < o:
                return False
            if s > o:
                return True
        return False


def run(directory):
    dirs = os.listdir(directory)
    versions = []
    for d in dirs:
        v = Version(d)
        versions.append(v)
    versions.sort(reverse=True)
    print(versions[0].get_dirname())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='Directory containing metadata-schema example version directories', required=True)
    args = parser.parse_args()
    run(args.d)
