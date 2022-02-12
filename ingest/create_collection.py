#!/usr/bin/env python

import argparse
import json

import pymongo
from pymongo import MongoClient

from util import create_collection


def main():
    parser = argparse.ArgumentParser(description="ingest newville data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://localhost:27017/?authSource=admin",
    )
    parser.add_argument("--mongo_username", default="root")
    parser.add_argument("--mongo_password", default="example")
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--schema", default="schema.json")
    parser.add_argument("--collection", default="newville")
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    client = MongoClient(
        args.mongo_uri, username=args.mongo_username, password=args.mongo_password
    )
    db = client[args.db]

    with open(args.schema) as f:
        schema = json.load(f)

    c = create_collection(db, args.collection, schema, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
