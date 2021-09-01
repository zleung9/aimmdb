import sys

import dask.array
import dask.dataframe
import numpy as np
import pandas as pd 
import xarray as xr
import uuid
import itertools

import json

import pymongo
from bson.objectid import ObjectId

import collections.abc

from dataclasses import dataclass

from tiled.utils import import_object, DictView
from tiled.trees.utils import IndexersMixin, UNCHANGED
from tiled.readers.xarray import DatasetAdapter

from tiled.query_registration import QueryTranslationRegistry, register

from tiled.trees.in_memory import Tree
from tiled.readers.dataframe import DataFrameAdapter

@register(name="raw_mongo")
@dataclass
class RawMongo:
    """
    Run a MongoDB query against a given collection.
    """

    query: str  # We cannot put a dict in a URL, so this a JSON str.

    def __init__(self, query):
        if isinstance(query, collections.abc.Mapping):
            query = json.dumps(query)
        self.query = query

def _get_database(uri):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri)
        return client.get_database()

class MongoCollectionTree(collections.abc.Mapping, IndexersMixin):

    # Define classmethods for managing what queries this Tree knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    @classmethod
    def from_uri(
        cls,
        uri,
        collection_name,
        *,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        ):

        db = _get_database(uri)
        collection = db.get_collection(collection_name)

        return cls(collection,
                   metadata=metadata,
                   access_policy=access_policy,
                   authenticated_identity=authenticated_identity)

    def __init__(self, 
            collection,
            metadata=None,
            access_policy=None,
            authenticated_identity=None,
            queries=None):

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
        combined = self._queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def _build_dataset(self, doc):
        raise NotImplementedError

    def __len__(self):
        return self._collection.count_documents(self._build_mongo_query())

    def __getitem__(self, key):
        query = self._build_mongo_query({"_id" : ObjectId(key)})
        doc = self._collection.find_one(query)
        if doc is None:
            raise KeyError(key)
        return self._build_dataset(doc)

    def __iter__(self):
        for doc in self._collection.find(self._build_mongo_query()):
            yield str(doc["_id"])

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            raise NotImplementedError

        tree = self.new_variation(authenticated_identity=identity)
        return tree

    def new_variation(
        self,
        authenticated_identity=UNCHANGED,
        queries=UNCHANGED
    ):
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        if queries is UNCHANGED:
            queries = self._queries

        return type(self)(
            collection = self._collection,
            metadata = self._metadata,
            access_policy = self._access_policy,
            authenticated_identity = authenticated_identity,
            queries = queries
        )

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def search(self, query):
        """
        Return a Tree with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self._collection.find(self._build_mongo_query()).skip(skip).limit(limit):
            _id = str(doc["_id"])
            yield _id


    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self._collection.find(self._build_mongo_query()).skip(skip).limit(limit):
            _id = str(doc["_id"])
            dset = self._build_dataset(doc)
            yield (_id, dset)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"

        doc = next(self._collection.find(self._build_mongo_query()).skip(index).limit(1))
        _id = str(doc["_id"])
        dset = self._build_dataset(doc)
        return (_id, dset)

def raw_mongo(query, tree):
    return tree.new_variation(
        queries=tree._queries + [json.loads(query.query)],
    )

class FEFFXASTree(MongoCollectionTree):
    def _build_dataset(self, doc):
        sp = doc["spectrum"]
        metadata_keys = set(doc.keys()) - set(["spectrum", "_id"])
        metadata = {k : doc[k] for k in metadata_keys}
        dset = xr.Dataset(
          data_vars = dict(
            mu=("energy", sp["mu"]),
            mu0=("energy", sp["mu0"]),
            chi=("k", sp["chi"])
          ),
          coords=dict(
            energy=sp["energies"],
            relative_energy=sp["relative_energies"],
            k=sp["wavenumber"]
          ))
        return DatasetAdapter(dset, metadata=metadata)

class QuantyXESTree(MongoCollectionTree):
    def _build_dataset(self, doc):
        metadata_keys = set(doc.keys()) - set(["mu", "energies", "_id"])
        metadata = {k : doc[k] for k in metadata_keys}
        dset = xr.Dataset(
                data_vars = dict(mu = ("energy", doc["mu"])),
                coords=dict(energy=doc["energies"])
                )
        return DatasetAdapter(dset, metadata=metadata)

FEFFXASTree.register_query(RawMongo, raw_mongo)
QuantyXESTree.register_query(RawMongo, raw_mongo)


df = DataFrameAdapter.from_pandas(
        pd.DataFrame({
            "A" : np.random.rand(200),
            "B" : np.random.rand(200),
            "C" : np.random.rand(200),
            "D" : np.random.rand(200)
            }),
        npartitions=1
        )
test = Tree({"test" : df})
