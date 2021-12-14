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


def walk(node, path, f):
    for k, n in node.items():
        child_path = path + [k]
        f(n, child_path)
        if type(n) is tiled.client.node.Node:
            walk(n, child_path, f)


def main():
    parser = argparse.ArgumentParser(description="ingest heald data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://root:example@localhost:27017/?authSource=admin",
    )
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--collection", default="heald")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--subpath", default="normalized")
    parser.add_argument("uri")

    args = parser.parse_args()

    client = from_uri(args.uri)
    root = client[args.subpath]

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    with open("schema.json") as f:
        schema = json.load(f)

    collection_name = f"{args.collection}_{args.subpath}"
    c = create_collection(db, collection_name, schema, overwrite=args.overwrite)

    i = 0

    def visitor(n, p):
        nonlocal i
        if type(n) is tiled.client.node.Node:
            pass
        else:
            df = n.read()
            metadata = dict(**n.metadata)
            uid = "-".join(p)
            print(f"{i}: {uid=}")
            i += 1
            element = metadata["common"]["element"]

            common = {
                "element": element,
                "spec": "heald",
                "uid": uid,
            }
            metadata["common"] = common

            data = {
                "media_type": "application/x-parquet",
                "structure_family": "dataframe",
                "blob": serialize_parquet(df).tobytes(),
            }

            doc = {"data": data, "metadata": metadata}
            c.insert_one(doc)

    walk(root, [], visitor)


if __name__ == "__main__":
    main()
