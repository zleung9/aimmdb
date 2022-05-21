import numpy as np
import pandas as pd
import pytest
import pydantic

from tiled.client import from_tree

import aimmdb
from aimmdb.adapters.mongo import MongoAdapter
from aimmdb.queries import RawMongo
from aimmdb.access import SimpleAccessPolicy
from aimmdb.schemas import Document, XASDocument
from tiled.authenticators import DictionaryAuthenticator

from .utils import fail_with_status_code

def test_basic(tmpdir):
    data_directory = tmpdir / "data"
    data_directory.mkdir()
    tree = MongoAdapter.from_mongomock(data_directory)

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


def test_access(enter_password, tmpdir):
    data_directory = tmpdir / "data"
    data_directory.mkdir()

    # alice can read and write
    # bob can read
    # joe is not listed and therefore cannot see anything
    access_policy = SimpleAccessPolicy(
        access_lists={"alice": "rw", "bob": "r"}, provider="toy"
    )

    tree = MongoAdapter.from_mongomock(data_directory, access_policy=access_policy)
    users_to_passwords = {"alice": "secret1", "bob": "secret2", "joe": "secret3"}

    authenticator = DictionaryAuthenticator(users_to_passwords=users_to_passwords)
    providers = [{"provider": "toy", "authenticator": authenticator}]
    authentication = {"providers": providers, "allow_anonymous_access": False}
    server_settings = {"database": {"uri": f"sqlite:///{tmpdir}/db.sqlite"}}

    with enter_password(users_to_passwords["alice"]):
        c_alice = from_tree(
            tree,
            username="alice",
            auth_provider="toy",
            authentication=authentication,
            server_settings=server_settings,
            token_cache=None,
        )

    with enter_password(users_to_passwords["bob"]):
        c_bob = from_tree(
            tree,
            username="bob",
            auth_provider="toy",
            authentication=authentication,
            server_settings=server_settings,
            token_cache=None,
        )

    with enter_password(users_to_passwords["joe"]):
        c_joe = from_tree(
            tree,
            username="joe",
            auth_provider="toy",
            authentication=authentication,
            server_settings=server_settings,
            token_cache=None,
        )

    assert len(c_alice) == 0
    assert len(c_bob) == 0
    assert len(c_joe) == 0

    assert c_alice.context.whoami()["identities"][0]["id"] == "alice"
    assert c_bob.context.whoami()["identities"][0]["id"] == "bob"
    assert c_joe.context.whoami()["identities"][0]["id"] == "joe"

    # alice is anble to write
    x = np.random.rand(100, 100)
    key0 = c_alice.write_array(x, {})
    assert len(c_alice) == 1

    # bob observes alice's write
    c_bob._cached_len = None  # invalidate length cache
    assert len(c_bob) == 1

    # bob is not able to write
    with fail_with_status_code(403):
        _ = c_bob.write_array(x, {})

    # bob can read alice's write
    node = c_bob[key0]
    np.testing.assert_equal(x, node.read())

    # joe does not observe the write
    c_joe._cached_len = None  # invalidate length cache
    assert len(c_joe) == 0

def test_validation(enter_password, tmpdir):
    data_directory = tmpdir / "data"
    data_directory.mkdir()
    spec_to_document_model = {"XAS" : XASDocument, "XAS_" : XASDocument}
    tree = MongoAdapter.from_mongomock(data_directory, spec_to_document_model=spec_to_document_model)

    api_key = "secret"
    c = from_tree(
        tree, api_key=api_key, authentication={"single_user_api_key": api_key}
    )
    assert type(c) == aimmdb.client.MongoCatalog

    df = pd.DataFrame({"a" : np.random.rand(100), "b" : np.random.rand(100)})
    x = np.random.rand(100, 100)
    xdi_element = {"symbol" : "Au", "edge": "K"}
    metadata = {"element" : xdi_element, "dataset" : "foo"}

    key = c.write_dataframe(df, metadata, specs=["XAS", "FOO"])
    node = c[key]

    pd.testing.assert_frame_equal(df, node.read())
    assert {k: node.metadata[k] for k in metadata} == metadata

    # can't write array with XAS spec
    with fail_with_status_code(400):
        c.write_array(x, metadata, specs=["XAS"])

    # can't write dataframe with missing metadata
    metadata_missing_dataset = {"element" : xdi_element}
    with fail_with_status_code(400):
        c.write_dataframe(df, metadata_missing_dataset, specs=["XAS"])

    metadata_missing_element = {"dataset" : "foo"}
    with fail_with_status_code(400):
        c.write_dataframe(df, metadata_missing_element, specs=["XAS"])

    # can't write data with specs which match multiple keys in spec_to_document_model
    with fail_with_status_code(400):
        c.write_dataframe(df, metadata_missing_element, specs=["XAS", "XAS_"])

    # check that failed writes are not visible to client
    c._cached_len = None # invalidate length cache
    assert len(c) == 1

    # we can write arbitrary data if the spec is unspecified
    key = c.write_array(x, {}, specs=[])
    node = c[key]
    np.testing.assert_equal(x, node.read())

    key = c.write_dataframe(df, {}, specs=[])
    node = c[key]
    pd.testing.assert_frame_equal(df, node.read())


def main():
    pytest.main()


if __name__ == "__main__":
    main()
