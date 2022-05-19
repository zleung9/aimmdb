import time

import numpy as np
import pandas as pd
import pytest
from tiled.client import from_tree

import aimmdb
from aimmdb.adapters.mongo import MongoAdapter
from aimmdb.queries import RawMongo
from aimmdb.access import AIMMAccessPolicy


@pytest.fixture
def tree(tmp_path):
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    access_policy = AIMMAccessPolicy({}, provider=None)
    return MongoAdapter.from_mongomock(data_directory, access_policy=access_policy)


def test_spike(tree):
    api_key = "secret"
    c = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )
    assert type(c) == aimmdb.client.MongoCatalog

    assert len(c) == 0

    x = np.random.rand(100, 100)
    metadata = {"foo": "bar"}
    key0 = c.write_array(x, metadata)
    assert len(c) == 1

    node = c[key0]
    np.testing.assert_equal(x, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata

    df = pd.DataFrame({"a": np.random.rand(100), "b": np.random.rand(100)})
    metadata = {"a": 1, "b": 2}
    key1 = c.write_dataframe(df, metadata)
    assert len(c) == 2

    node = c[key1]
    pd.testing.assert_frame_equal(df, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata

    del c[key0]
    assert len(c) == 1

    del c[key1]
    assert len(c) == 0


def main():
    pytest.main()


if __name__ == "__main__":
    main()
