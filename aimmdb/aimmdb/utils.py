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


def get_share_aimmdb_path():
    """Walk up until we find share/aimmdb"""
    import sys
    from os.path import abspath, dirname, exists, join, split

    path = abspath(dirname(__file__))
    starting_points = [path]
    if not path.startswith(sys.prefix):
        starting_points.append(sys.prefix)
    for path in starting_points:
        # walk up, looking for prefix/share/jupyter
        while path != "/":
            share_tiled = join(path, "share", "aimmdb")
            if exists(
                join(share_tiled, ".identifying_file_5fde776bf5ee64081be861ce6f02490b")
            ):
                # We have the found the right directory,
                # or one that is trying very hard to pretend to be!
                return share_tiled
            path, _ = split(path)
    # Give up
    return ""


# Package managers can just override this with the appropriate constant
SHARE_AIMMDB_PATH = get_share_aimmdb_path()
