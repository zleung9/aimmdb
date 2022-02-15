import base64
import collections.abc
import io
import itertools
import json
import sys
import uuid
from dataclasses import dataclass

import ariadne
import dask.array
import dask.dataframe
import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymongo
import xarray
from ariadne import ObjectType, QueryType, gql, make_executable_schema
from ariadne.asgi import GraphQL
from bson.objectid import ObjectId
from fastapi import APIRouter, Depends, Request
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.utils import IndexersMixin
from tiled.adapters.xarray import DataArrayAdapter
from tiled.query_registration import QueryTranslationRegistry, register
from tiled.server.authentication import get_current_principal
from tiled.utils import UNCHANGED, DictView, import_object


def deserialize_parquet(data):
    reader = pa.BufferReader(data)
    table = pq.read_table(reader)
    return table.to_pandas()


@register(name="raw_mongo")
@dataclass
class RawMongoQuery:
    """
    Run a MongoDB query against a given collection.
    """

    query: str  # We cannot put a dict in a URL, so this a JSON str.

    def __init__(self, query):
        if isinstance(query, collections.abc.Mapping):
            query = json.dumps(query)
        self.query = query


def _get_database(uri, username, password):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri, username=username, password=password)
        return client.get_database()


class MongoCollectionTree(collections.abc.Mapping, IndexersMixin):

    structure_family = "node"

    # Define classmethods for managing what queries this Tree knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    @classmethod
    def from_uri(
        cls,
        uri,
        username,
        password,
        collection_name,
        *,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
    ):

        db = _get_database(uri, username, password)
        collection = db.get_collection(collection_name)

        return cls(
            collection,
            metadata=metadata,
            access_policy=access_policy,
            authenticated_identity=authenticated_identity,
        )

    def __init__(
        self,
        collection,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        queries=None,
        path="/",
    ):

        self._collection = collection

        self._metadata = metadata or {}
        if isinstance(access_policy, str):
            access_policy = import_object(access_policy)
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Tree."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity

        self._queries = list(queries or [])
        self._path = path

        super().__init__()

    @property
    def collection(self):
        return self._collection

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Tree."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def _build_mongo_query(self, *queries):
        combined = [{"path": {"$regex": f"^{self._path}[^/]*$"}}]
        combined += self._queries
        combined += list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def _build_node(self, doc):
        raise NotImplementedError

    def __len__(self):
        return self._collection.count_documents(self._build_mongo_query())

    def __getitem__(self, key):
        query = self._build_mongo_query({"name": key})
        docs = list(self._collection.find(query))

        if len(docs) == 0:
            raise KeyError(f"{key} not found")

        if len(docs) > 1:
            raise KeyError(f"{key} matched multipled records")

        return self._build_node(docs[0])

    def __iter__(self):
        for doc in self._collection.find(self._build_mongo_query(), {"name": 1}):
            yield str(doc["name"])

    def authenticated_as(self, identity):
        # TODO understand if we should still have this code
        #        if self._authenticated_identity is not None:
        #            raise RuntimeError(
        #                f"Already authenticated as {self.authenticated_identity}"
        #            )
        if self._access_policy is not None:
            raise NotImplementedError

        tree = self.new_variation(authenticated_identity=identity)
        return tree

    def new_variation(
        self, authenticated_identity=UNCHANGED, queries=UNCHANGED, path=UNCHANGED
    ):
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        if queries is UNCHANGED:
            queries = self._queries
        if path is UNCHANGED:
            path = self._path

        return type(self)(
            collection=self._collection,
            metadata=self._metadata,
            access_policy=self._access_policy,
            authenticated_identity=authenticated_identity,
            queries=queries,
            path=path,
        )

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def search(self, query):
        """
        Return a Tree with a subset of the mapping.
        """
        print(f"{query=}")
        return self.query_registry(query, self)

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        query = self._build_mongo_query()
        for doc in self._collection.find(query, {"name": 1}).skip(skip).limit(limit):
            k = str(doc["name"])
            yield k

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        # FIXME don't load full document with all the data here
        for doc in (
            self._collection.find(self._build_mongo_query()).skip(skip).limit(limit)
        ):
            k = str(doc["name"])
            dset = self._build_node(doc)
            yield (k, dset)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"

        doc = next(
            self._collection.find(self._build_mongo_query()).skip(index).limit(1)
        )
        k = str(doc["name"])
        dset = self._build_node(doc)
        return (k, dset)


class AIMMTree(MongoCollectionTree):
    def __init__(
        self,
        collection,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        queries=None,
        path="/",
    ):

        super().__init__(
            collection, metadata, access_policy, authenticated_identity, queries, path
        )

    def _build_dataset(self, doc):

        metadata = doc["metadata"]

        # FIXME use tiled.utils.OneShotCachedMap to make lazy?
        mapping = {}
        for m in doc["measurements"]:
            name = m["name"]
            df = deserialize_parquet(m["data"]["blob"])
            metadata = m["metadata"]
            metadata["element"] = m["element"]

            mapping[name] = DataFrameAdapter.from_pandas(
                df, metadata=metadata, npartitions=1
            )

        return MapAdapter(mapping, metadata=doc["metadata"])

    def _build_node(self, doc):
        if doc["folder"]:
            name = doc["name"]
            return self.new_variation(path=f"{self._path}{name}/")
        else:
            return self._build_dataset(doc)

    def read(self, fields=None):
        if fields is not None:
            raise NotImplementedError
        return self


def run_raw_mongo_query(query, tree):
   query = json.loads(query.query)
   query = {"$or" : [{"folder" : True}, query]}
   return tree.new_variation(queries=tree._queries + [query])
AIMMTree.register_query(RawMongoQuery, run_raw_mongo_query)

def walk(node, pre=None):
    pre = pre[:] if pre else []

    if isinstance(node, AIMMTree):
        for k, v in node.items():
            yield from walk(v, pre + [k])
        if node.metadata:
            yield from walk(node.metadata, pre + ["_metadata"])
    elif isinstance(node, collections.abc.Mapping):
        for k, v in node.items():
            yield from walk(v, pre + [k])
    elif isinstance(node, DataFrameAdapter):
        df = node.read()
        yield (df.to_numpy(), pre + ["_data"])
        metadata = {**node.metadata}
        metadata["columns"] = list(df.columns)
        yield from walk(metadata, pre + ["_metadata"])
    else:
        yield (node, pre)


def serialize_hdf5(node, metadata):
    buffer = io.BytesIO()
    with h5py.File(buffer, mode="w") as file:
        for (x, pre) in walk(node):
            path = "/".join(pre)
            if x is not None:
                file[path] = x
            else:
                file[path] = h5py.Empty("f")

    return buffer.getbuffer()
