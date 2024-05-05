#!/usr/bin/env python3

"""
Utility to copy indexes from one MDB instance to another.
"""

import argparse

import pymongo


def parse_args() -> argparse.Namespace:
    """Parse some args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source-uri", help="Source MDB instance to read indexes from.")
    parser.add_argument(
        "dest-uri", help="Destination MDB instance to write indexes to."
    )
    parser.add_argument(
        "-r",
        "--remove-dest-indexes",
        help="Remove all indexes in the destination MDB instance.",
    )
    return parser.parse_args()


def get_conn(conn_string: str) -> pymongo.database.Database:  # type: ignore[type-arg]
    """Get the mongodb connection."""
    return pymongo.MongoClient(conn_string).get_database()


def copy_indexes(
    source_conn: pymongo.database.Database,  # type: ignore[type-arg]
    dest_conn: pymongo.database.Database,  # type: ignore[type-arg]
) -> None:
    """Copy indexes!."""
    # Iterate over each database
    for db_name in source_conn.list_database_names():  # type: ignore[attr-defined]
        source_db = source_conn[db_name]

        # Iterate over each collection in the database
        for collection_name in source_db.list_collection_names():
            collection = source_db[collection_name]

            # Grab all indexes from the collection
            indexes = collection.list_indexes()

            dest_db = dest_conn.get_database(db_name)
            dest_collection = dest_db.get_collection(collection_name)

            # Create indexes in the dest database
            for index in indexes:
                dest_collection.create_index(index["key"])


def remove_dest_indexes(mdb_conn: pymongo.database.Database) -> None:  # type: ignore[type-arg]
    """Delete all indexes in a MDB instnace."""
    # Iterate over each database
    for db_name in mdb_conn.list_database_names():  # type: ignore[attr-defined]
        dest_db = mdb_conn[db_name]

        # Iterate over each collection in the database
        for collection_name in dest_db.list_collection_names():
            collection = dest_db[collection_name]

            # Drop all indexes in the collection
            collection.drop_indexes()


def main() -> None:
    """Main."""
    args = parse_args()
    source_conn = get_conn(args.source_uri)
    dest_conn = get_conn(args.dest_uri)

    if args.remove_dest_indexes:
        remove_dest_indexes(dest_conn)
    copy_indexes(source_conn, dest_conn)


if __name__ == "__main__":
    main()
