import argparse
from utils.utils import (
    retrive_identities,
    retrive_subgraphs,
    write_subgraphs,
    hash_identity,
)


parser = argparse.ArgumentParser()
parser.add_argument("config_path")


def main():
    ids = retrive_subgraphs()
    subgraphs_to_insert = retrive_subgraphs(seed_identities=ids)
    write_subgraphs(subgraphs_to_insert)


if __name__ == "__main__":
    main()
