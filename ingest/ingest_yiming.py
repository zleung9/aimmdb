#!/usr/bin/env python

import argparse
import pathlib
import subprocess
import json

import pandas as pd

import pymongo
from pymongo import MongoClient

from util import serialize_parquet
from util import create_collection

def main():
    parser = argparse.ArgumentParser(description="ingest yiming xas data")
    parser.add_argument("--mongo_uri", default="mongodb://root:example@localhost:27017/?authSource=admin")
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--collection", default="xas_feff_nmc")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("data_path")

    args = parser.parse_args()

    path = pathlib.Path(args.data_path)
    assert path.is_file()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    assert f"{args.collection}_import" not in db.list_collection_names()

    cmd = ["mongoimport", "--jsonArray", "-d", args.db, "-c", f"{args.collection}_import", args.mongo_uri, args.data_path]
    subprocess.run(cmd)
    c_import = db[f"{args.collection}_import"]

    try:
        with open("schema.json") as f:
            schema = json.load(f)

        c = create_collection(db, args.collection, schema, overwrite=args.overwrite)

        for doc in c_import.find({}):
            sp = doc["spectrum"]

            metadata_keys = set(doc.keys()) - set(["spectrum", "_id"])
            metadata = {k : doc[k] for k in metadata_keys}
            common = {"element" : {"symbol" : metadata["absorbing_atom"], "edge" : metadata["EDGE"]}}
            metadata["common"] = common

            df = pd.DataFrame({
                "energy" : sp["energies"],
                "energy_relative" : sp["relative_energies"],
                "k" : sp["wavenumber"],
                "mu" : sp["mu"],
                "mu0" : sp["mu0"],
                "chi" : sp["chi"]
                })

            doc_ = {"table" : serialize_parquet(df).tobytes(), "metadata" : metadata}
            c.insert_one(doc_)
    finally:
        db.drop_collection(c_import)

if __name__ == "__main__":
    main()
