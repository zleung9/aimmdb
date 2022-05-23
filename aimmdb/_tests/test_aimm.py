import numpy as np
import pandas as pd
import pytest
import pydantic

from tiled.client import from_tree

import aimmdb
from aimmdb.adapters.aimm import AIMMCatalog, key_translation
from aimmdb.queries import RawMongo
from aimmdb.access import SimpleAccessPolicy
from aimmdb.schemas import XASDocument
from tiled.authenticators import DictionaryAuthenticator

from .utils import fail_with_status_code


def test_basic(tmpdir):
    data_directory = tmpdir / "data"
    data_directory.mkdir()

    spec_to_document_model = {"XAS": XASDocument}
    dataset_to_specs = {"xas": ["XAS"]}

    tree = AIMMCatalog.from_mongomock(
        data_directory,
        spec_to_document_model=spec_to_document_model,
        dataset_to_specs=dataset_to_specs,
    )

    api_key = "secret"
    c = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )
    assert type(c) == aimmdb.client.AIMMCatalog
    assert set(c) == set(key_translation.keys())

    x = np.random.rand(100, 100)
    metadata = {"foo": "bar"}

    # can't write without specifying dataset
    with fail_with_status_code(400):
        key0 = c["uid"].write_array(x, metadata)

    metadata.update(dataset="sandbox1")
    key0 = c["uid"].write_array(x, metadata)
    assert set(c["dataset"]["sandbox1"]["uid"]) == {key0}
    node = c["uid"][key0]
    np.testing.assert_equal(x, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata


    metadata.update(dataset="sandbox2")
    df = pd.DataFrame({"a": np.random.rand(100), "b": np.random.rand(100)})
    key1 = c["uid"].write_dataframe(df, metadata)
    assert set(c["dataset"]["sandbox2"]["uid"]) == {key1}
    node = c["uid"][key1]
    pd.testing.assert_frame_equal(df, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata

    assert set(c["uid"]) == {key0, key1}

    # we can write data with xas metadata to the xas dataset
    metadata.update(dataset="xas", element={"symbol" : "Au", "edge" : "K"})
    key2 = c["uid"].write_dataframe(df, metadata, specs=["XAS"])
    node = c["uid"][key2]
    pd.testing.assert_frame_equal(df, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata

    # however we fail if we don't include the spec
    with fail_with_status_code(400):
        _ = c["uid"].write_dataframe(df, metadata)

    # or if we try to write an array
    with fail_with_status_code(400):
        _ = c["uid"].write_array(x, metadata, specs=["XAS"])

    # or if we do not have required metadata elements
    del metadata["element"]
    with fail_with_status_code(400):
        _ = c["uid"].write_dataframe(df, metadata, specs=["XAS"])

    assert set(c["uid"]) == {key0, key1, key2}
    assert set(c["dataset"]["xas"]["uid"]) == {key2}

    for k in [key0, key1, key2]:
        del c["uid"][k]

    assert len(c["uid"]) == 0

def main():
    pytest.main()


if __name__ == "__main__":
    main()
