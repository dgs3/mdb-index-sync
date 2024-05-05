#!/usr/bin/env python3

"""
Utility to copy indexes from one MDB instance to another.
"""

import argparse
import typing

import pymongo

IGNORE_DBS = ["admin", "config", "local"]

DBClient = pymongo.MongoClient[dict[str, typing.Any]]


def parse_args() -> argparse.Namespace:
    """Parse some args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-r",
        "--remove-dest-indexes",
        action="store_true",
        help="Remove all indexes in the destination MDB instance.",
    )
    parser.add_argument("source_uri", help="Source MDB instance to read indexes from.")
    parser.add_argument(
        "dest_uri", help="Destination MDB instance to write indexes to."
    )
    return parser.parse_args()


def get_client(conn_string: str) -> DBClient:
    """Get the mongodb connection."""
    return pymongo.MongoClient(conn_string)


def get_db_names(source_client: DBClient) -> typing.Generator[str, None, None]:
    """Get all usable db names from a DBClient."""
    for db_name in source_client.list_database_names():
        if db_name in IGNORE_DBS:
            continue
        yield db_name


def copy_indexes(source_client: DBClient, dest_client: DBClient) -> None:
    """Copy indexes!."""
    # Iterate over each database
    for db_name in get_db_names(source_client):
        source_db = source_client[db_name]

        # Iterate over each collection in the database
        for collection_name in source_db.list_collection_names():
            collection = source_db[collection_name]

            # Grab all indexes from the collection
            indexes = collection.list_indexes()

            dest_db = dest_client.get_database(db_name)
            dest_collection = dest_db.get_collection(collection_name)

            # Create indexes in the dest database
            for index in indexes:
                dest_collection.create_index(index["key"], name=index["name"])


def remove_dest_indexes(db_client: DBClient) -> None:
    """Delete all indexes in a DBClient."""
    # Iterate over each database
    for db_name in get_db_names(db_client):
        dest_db = db_client[db_name]

        # Iterate over each collection in the database
        for collection_name in dest_db.list_collection_names():
            collection = dest_db[collection_name]

            # Drop all indexes in the collection
            collection.drop_indexes()


def main() -> None:
    """Main."""
    args = parse_args()
    source_client = get_client(args.source_uri)
    dest_client = get_client(args.dest_uri)

    if args.remove_dest_indexes:
        remove_dest_indexes(dest_client)
    copy_indexes(source_client, dest_client)


if __name__ == "__main__":
    main()
