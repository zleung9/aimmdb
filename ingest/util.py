from collections import OrderedDict

import pymongo

import pyarrow as pa
import pyarrow.parquet as pq

import h5py


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
    db[collection].create_index(
        [("name", pymongo.ASCENDING), ("parent", pymongo.ASCENDING)], unique=True
    )

    return db[collection]


# TODO should there be an explicit root node?
def rm_path(c, path):
    r = get_path(c, path)
    id = r["_id"]
    if id:
        c.delete_many({"ancestors": id})
        c.delete_one({"_id": id})


def get_path(c, path):
    parent = None
    for i, p in enumerate(path):
        result = c.find_one({"parent": parent, "name": p})
        if result is None:
            raise KeyError("path {} does not exist".format("/".join(path[: (i + 1)])))
        parent = result["_id"]
    return result


def mk_path(c, path):
    ancestors = []
    parent = None

    for p in path:
        doc = c.find_one({"parent": parent, "name": p})
        if doc:
            parent = doc["_id"]
            ancestors.append(parent)
        else:
            doc = {
                "name": p,
                "leaf": False,
                "ancestors": ancestors,
                "parent": parent,
                "content": None,
            }
            result = c.insert_one(doc)
            parent = result.inserted_id
            ancestors.append(parent)

    return parent


def get_children(c, path, recursive=False):
    id = get_path(c, path)["_id"]
    if recursive:
        return c.find({"ancestors": id})
    else:
        return c.find({"parent": id})


def tree(c, parent=None, depth=0):
    out = {}
    for x in c.find({"parent": parent, "leaf": False}, {"_id": 1, "name": 1}):
        out[x["name"]] = tree(c, x["_id"], depth=depth + 1)

    return out


# read a group recursively into memory
def read_group(g, jsoncompat=False):
    out = {}
    for k, v in g.items():
        if isinstance(v, h5py.Group):  # group
            x = read_group(v, jsoncompat=jsoncompat)
        else:  # dataset
            if v.shape == ():  # scalar
                x = v[()]
                if type(x) is bytes:
                    x = x.decode("utf-8")
                else:
                    if jsoncompat:
                        x = x.item()
            else:  # array
                x = v[:]
                if jsoncompat:
                    x = x.tolist()
        out[k] = x
    return out
