#!/usr/bin/env python

import argparse
import pathlib
import json
from collections import defaultdict

import tiled.client
from tiled.client import from_uri

import pymongo
from pymongo import MongoClient

from util import serialize_parquet
from util import create_collection


def walk(client, node, name, path, ancestors):
    print(f"{path=}")
    if ancestors:
        parent = ancestors[-1]
    else:
        parent = None
    doc = {"name" : name, "parent" : parent, "ancestors" : ancestors}
    if type(node) is tiled.client.node.Node:
        doc.update(leaf=False, content=None)
        id_ = client.insert_one(doc).inserted_id
        for k, n in node.items():
            walk(client, n, k, path + [k], ancestors + [id_])
    else:
        df = node.read()
        metadata = dict(**node.metadata)
        data = {
            "media_type": "application/x-parquet",
            "structure_family": "dataframe",
            "blob": serialize_parquet(df).tobytes(),
        }
        doc.update(leaf=True, content={"data" : data, "metadata" : metadata})
        client.insert_one(doc)

def main():
    parser = argparse.ArgumentParser(description="ingest heald data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://localhost:27017/?authSource=admin",
    )
    parser.add_argument("--mongo_username", default="root")
    parser.add_argument("--mongo_password", default="example")
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--collection", default="spike")
    parser.add_argument("uri")

    args = parser.parse_args()

    tiled_root = from_uri(args.uri)

    client = MongoClient(args.mongo_uri, username=args.mongo_username, password=args.mongo_password)
    db = client[args.db]
    c = db[args.collection]

    walk(c, tiled_root, "heald", [], [])

if __name__ == "__main__":
    main()
