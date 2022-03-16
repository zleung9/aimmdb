import importlib
import json
from collections import OrderedDict

import h5py

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

_ELEMENT_DATA = None

def get_element_data():
    # load on first access
    global _ELEMENT_DATA
    if _ELEMENT_DATA is None:
        fname = importlib.resources.files("aimmdb") / "data" / "elements.json"
        with open(fname, "r") as f:
            data = json.load(f)
        _ELEMENT_DATA = data
    return _ELEMENT_DATA
