from collections import OrderedDict

import h5py
import pyarrow as pa
import pyarrow.parquet as pq
import pymongo

from models import Folder


def mk_path(c, path):
    for i, p in enumerate(path):
        path_str = "/" + "/".join(path[0 : i + 1])
        doc = c.find_one({"path": path_str})
        if doc is None:
            doc = Folder(name=p, folder=True, path=path_str, metadata={})
            result = c.insert_one(doc.dict())
    return doc


# read an hdf5 group recursively into memory
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
