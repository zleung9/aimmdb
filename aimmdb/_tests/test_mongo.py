import time

import numpy as np
import pandas as pd
import pytest
from tiled.client import from_tree

import aimmdb
from aimmdb.adapters.mongo import MongoAdapter
from aimmdb.queries import RawMongo


@pytest.fixture
def tree(tmp_path):
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    return MongoAdapter.from_mongomock(data_directory)


def test_spike(tree):
    assert hasattr(tree, "keys_indexer")

    api_key = "secret"
    c = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )
    assert type(c) == aimmdb.client.MongoCatalog

    assert len(c) == 0

    x = np.random.rand(100, 100)
    metadata = {"foo": "bar"}
    c.write_array(x, metadata)

    time.sleep(1)
    assert len(c) == 1  # FIXME why is this not immediately observed

    k = c.keys_indexer[0]
    v = c[k]

    np.testing.assert_equal(x, v.read())
    assert dict(**v.metadata) == metadata


def main():
    pytest.main()


if __name__ == "__main__":
    main()
