#!/usr/bin/env python

import argparse
import json
import pathlib
from collections import defaultdict

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm

from util import create_collection, serialize_parquet

target_elements_groups = [
    ("Ti", "O"),
    ("V", "O"),
    ("Cr", "O"),
    ("Mn", "O"),
    ("Fe", "O"),
    ("Co", "O"),
    ("Ni", "O"),
    ("Cu", "O"),
]


def main():
    parser = argparse.ArgumentParser(description="ingest torrisi data")
    parser.add_argument(
        "--mongo_uri",
        default="mongodb://root:example@localhost:27017/?authSource=admin",
    )
    parser.add_argument("--db", default="aimm")
    parser.add_argument("--collection", default="torrisi")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("data_path")

    args = parser.parse_args()

    path = pathlib.Path(args.data_path)
    assert path.exists()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    with open("schema.json") as f:
        schema = json.load(f)

    c = create_collection(db, args.collection, schema, overwrite=args.overwrite)

    counts = defaultdict(int)

    for pair in target_elements_groups:
        target_file = path / f"{pair[0]}_XY.json"
        print(target_file)
        assert path.exists()

        with open(target_file, "r") as f:
            for line in tqdm(f.readlines()):
                data = json.loads(line)
                df = pd.DataFrame({"energy": data["E"], "mu": data["mu"]})

                bader = data.get("bader")
                avg_nn_dists = data.get("avg_nn_dists")
                coordination = data.get("coordination")

                symbol = pair[0]
                edge = "K"

                uid_prefix = f"{symbol}-{edge}"
                uid_suffix = str(counts[uid_prefix])
                uid = f"{uid_prefix}-{uid_suffix}"
                counts[uid_prefix] += 1

                common = {
                    "element": {"symbol": symbol, "edge": edge},
                    "spec": "feff",
                    "uid": uid,
                }

                metadata = {
                    "bader": bader,
                    "avg_nn_dists": avg_nn_dists,
                    "coordination": coordination,
                    "common": common,
                }

                df = pd.DataFrame({"energy": data["E"], "mu": data["mu"]})

                data = {
                    "media_type": "application/x-parquet",
                    "structure_family": "dataframe",
                    "blob": serialize_parquet(df).tobytes(),
                }

                doc = {"data": data, "metadata": metadata}
                c.insert_one(doc)


if __name__ == "__main__":
    main()
