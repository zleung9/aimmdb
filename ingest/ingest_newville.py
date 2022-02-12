#!/usr/bin/env python

import argparse
import pathlib
import json
import uuid

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

    tags = ["experiment", "newville", "xas", "xdi"]

    doc = {"name" : "newville", "leaf" : False, "ancestors" : [], "parent" : None, "content" : None}
    newville_id = c.insert_one(doc).inserted_id

    for f in tqdm(files):
        df, metadata = read_xdi(str(f))
        fields = metadata.pop("fields")
        metadata.update(**fields)

        metadata = {k.lower() : v for k,v in metadata.items()}

        # FIXME coerce keys to lower case?

        name = f.stem

        data = {
            "media_type": "application/x-parquet",
            "structure_family": "dataframe",
            "blob": serialize_parquet(df).tobytes(),
        }

        columns = list(df.columns)
        symbol = metadata["element"]["symbol"]
        edge = metadata["element"]["edge"]
        xdi = {
            "element": {"symbol": symbol, "edge": edge},
            "columns" : columns
        }
        sample_id = str(uuid.uuid4())
        internal = {"tags" : tags, "sample_id" : sample_id, "xdi" : xdi}

        content = {"data": data, "metadata": metadata, "internal" : internal}
        doc = {"name" : name, "leaf" : True, "ancestors" : [newville_id], "parent" : newville_id, "content" : content}
        c.insert_one(doc)

if __name__ == "__main__":
    main()
