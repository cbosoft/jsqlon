import sys

from . import Database

def main():
    """Simple function to use context manager of .Database to sync json and sqlite databases."""
    kw = {}
    if len(sys.argv) > 1:
        kw['path'] = sys.argv[1]
    with Database(**kw):
        pass


if __name__ == '__main__':
    main()
