#!/usr/bin/env python

import argparse
import json
import pathlib
import subprocess
from collections import defaultdict

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm

from util import create_collection, serialize_parquet


def get_symbol(doc):
    # first try the absorbing_atom field
    absorbing_atom = doc["absorbing_atom"]
    if type(absorbing_atom) is str:
        return absorbing_atom

    # next try metadata.absorbing_species
    absorbing_species = doc["metadata"]["absorbing_species"]
    if type(absorbing_species) is str:
        return absorbing_species

    # give up
    _id = doc["_id"]
    raise KeyError(
        f"unable to retrive element symbol from {_id}, {absorbing_atom=}, {absorbing_species=}"
    )


def main():
    parser = argparse.ArgumentParser(description="ingest yiming xas data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://root:example@localhost:27017/?authSource=admin",
    )
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

    cmd = [
        "mongoimport",
        "--jsonArray",
        "-d",
        args.db,
        "-c",
        f"{args.collection}_import",
        args.mongo_uri,
        args.data_path,
    ]
    subprocess.run(cmd)
    c_import = db[f"{args.collection}_import"]
    n = c_import.count_documents({})

    try:
        with open("schema.json") as f:
            schema = json.load(f)

        c = create_collection(db, args.collection, schema, overwrite=args.overwrite)

        counts = defaultdict(int)

        cursor = c_import.find({})
        for doc in tqdm(cursor, total=n):
            sp = doc["spectrum"]

            metadata_keys = set(doc.keys()) - set(["spectrum", "_id"])
            metadata = {k: doc[k] for k in metadata_keys}

            symbol = get_symbol(doc)
            edge = metadata["EDGE"]

            uid_prefix = f"{symbol}-{edge}"
            uid_suffix = str(counts[uid_prefix])
            uid = f"{uid_prefix}-{uid_suffix}"
            counts[uid_prefix] += 1

            common = {
                "element": {"symbol": symbol, "edge": edge},
                "spec": "feff",
                "uid": uid,
            }
            metadata["common"] = common

            df = pd.DataFrame(
                {
                    "energy": sp["energies"],
                    "energy_relative": sp["relative_energies"],
                    "k": sp["wavenumber"],
                    "mu": sp["mu"],
                    "mu0": sp["mu0"],
                    "chi": sp["chi"],
                }
            )

            data = {
                "media_type": "application/x-parquet",
                "structure_family": "dataframe",
                "blob": serialize_parquet(df).tobytes(),
            }

            doc_ = {"data": data, "metadata": metadata}
            try:
                c.insert_one(doc_)
            except pymongo.errors.WriteError as e:
                _id = doc["_id"]
                print(f"unable to import {_id}")
                print(e)

    finally:
        db.drop_collection(c_import)


if __name__ == "__main__":
    main()
