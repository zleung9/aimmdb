#!/usr/bin/env python

import argparse
import pathlib
import json

from tiled.examples.xdi import read_xdi

from tqdm import tqdm

import pymongo
from pymongo import MongoClient

from util import serialize_parquet

def main():
    parser = argparse.ArgumentParser(description="ingest newville data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://localhost:27017/?authSource=admin",
    )
    parser.add_argument("--mongo_username", default="root")
    parser.add_argument("--mongo_password", default="example")
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--collection", default="spike")
    parser.add_argument("data_path")

    args = parser.parse_args()

    path = pathlib.Path(args.data_path)
    assert path.exists()
    files = list(path.rglob("*.xdi"))
    print(f"found {len(files)} xdi files to ingest")

    client = MongoClient(args.mongo_uri, username=args.mongo_username, password=args.mongo_password)
    db = client[args.db]
    c = db[args.collection]

    specs = ["experiment", "newville", "xas"]

    doc = {"name" : "newville", "leaf" : False, "ancestors" : [], "parent" : None, "content" : None}
    newville_id = c.insert_one(doc).inserted_id

    for f in tqdm(files):
        df, metadata = read_xdi(str(f))
        fields = metadata.pop("fields")
        metadata.update(**fields)

        symbol = metadata["Element"]["symbol"]
        edge = metadata["Element"]["edge"]

        name = f.stem

        columns = list(df.columns)
        common = {
            "element": {"symbol": symbol, "edge": edge},
            "specs": specs,
            "columns" : columns
        }
        metadata["common"] = common

        data = {
            "media_type": "application/x-parquet",
            "structure_family": "dataframe",
            "blob": serialize_parquet(df).tobytes(),
        }

        content = {"data": data, "metadata": metadata}
        doc = {"name" : name, "leaf" : True, "ancestors" : [newville_id], "parent" : newville_id, "content" : content}
        c.insert_one(doc)

if __name__ == "__main__":
    main()
