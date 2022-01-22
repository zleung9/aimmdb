from collections import OrderedDict

import pymongo

import pyarrow as pa
import pyarrow.parquet as pq


def serialize_parquet(df):
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    buf = sink.getvalue()
    return memoryview(sink.getvalue())


def deserialize_parquet(data):
    reader = pa.BufferReader(data)
    table = pq.read_table(reader)
    return table.to_pandas()


def create_collection(db, collection, schema, overwrite=False):
    exists = collection in db.list_collection_names()

    if exists and not overwrite:
        print(f"collection {collection} already exists. exiting...")
        return
    elif exists and overwrite:
        print(f"collection {collection} already exists. overwriting...")
        db.drop_collection(collection)

    db.create_collection(collection)

    vexpr = {"$jsonSchema": schema}

    cmd = OrderedDict(
        [("collMod", collection), ("validator", vexpr), ("validationLevel", "strict")]
    )
    db.command(cmd)

    db[collection].create_index("ancestors")
    db[collection].create_index("parent")
    db[collection].create_index("content.metadata.common")
    db[collection].create_index([("name", pymongo.ASCENDING), ("parent", pymongo.ASCENDING)], unique=True)

    return db[collection]
