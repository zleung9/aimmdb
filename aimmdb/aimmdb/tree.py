import sys

import base64
import dask.array
import dask.dataframe
import numpy as np
import pandas as pd
import uuid
import itertools

import ariadne
from ariadne import ObjectType, QueryType, gql, make_executable_schema
from ariadne.asgi import GraphQL

from fastapi import APIRouter, Request, Depends

import json

import pymongo
from bson.objectid import ObjectId

import pyarrow as pa
import pyarrow.parquet as pq

import collections.abc

from dataclasses import dataclass

from tiled.utils import import_object, DictView, UNCHANGED
from tiled.adapters.utils import IndexersMixin
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter as Tree
from tiled.server.authentication import get_current_user

from tiled.query_registration import QueryTranslationRegistry, register

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

@register(name="element")
@dataclass
class ElementQuery:

    symbol: str
    edge: str

    def __init__(self, symbol, edge):
        self.symbol = symbol
        self.edge = edge

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

        return cls(collection,
                   metadata=metadata,
                   access_policy=access_policy,
                   authenticated_identity=authenticated_identity)

    def __init__(self,
            collection,
            metadata=None,
            access_policy=None,
            authenticated_identity=None,
            queries=None,
            parent=None):

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
        self._parent = parent


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
        combined = [{"parent" : self._parent}]
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
        query = self._build_mongo_query({"name" : key})
        docs = list(self._collection.find(query))

        if len(docs) == 0:
            raise KeyError(f"{key} not found")

        if len(docs) > 1:
            raise KeyError(f"{key} matched multipled records")

        return self._build_node(docs[0])

    def __iter__(self):
        for doc in self._collection.find(self._build_mongo_query()):
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
        self,
        authenticated_identity=UNCHANGED,
        queries=UNCHANGED,
        parent=UNCHANGED
    ):
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        if queries is UNCHANGED:
            queries = self._queries
        if parent is UNCHANGED:
            parent = self._parent

        return type(self)(
            collection = self._collection,
            metadata = self._metadata,
            access_policy = self._access_policy,
            authenticated_identity = authenticated_identity,
            queries = queries,
            parent = parent
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
            _id = str(doc["name"])
            yield _id


    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self._collection.find(self._build_mongo_query()).skip(skip).limit(limit):
            _id = str(doc["name"])
            dset = self._build_node(doc)
            yield (_id, dset)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"

        doc = next(self._collection.find(self._build_mongo_query()).skip(index).limit(1))
        _id = str(doc["name"])
        dset = self._build_node(doc)
        return (_id, dset)

class MongoXASTree(MongoCollectionTree):
    def __init__(self,
            collection,
            metadata=None,
            access_policy=None,
            authenticated_identity=None,
            queries=None,
            parent=None):

        super().__init__(collection, metadata, access_policy, authenticated_identity, queries, parent)

        type_defs = gql("""
            type Query {
                spectra(symbol: String, edge: String, offset: Int, limit: Int): [spectrum]
            }

            type spectrum_metadata {
                name: String!
                symbol: String!
                edge: String!
            }

            type spectrum {
                metadata: spectrum_metadata!
                data: String!
            }
        """)

        query = QueryType()
        spectrum = ObjectType("spectrum")
        spectrum_metadata = ObjectType("spectrum_metadata")

        @query.field("spectra")
        def resolve_xas_spectrum(obj, info, symbol=None, edge=None, offset=0, limit=50):
            query = {}
            if symbol:
                query["metadata.common.element.symbol"] = symbol
            if edge:
                query["metadata.common.element.edge"] = edge

            return [doc for doc in self._collection.find(query).skip(offset).limit(limit)]

        @spectrum.field("data")
        def resolve_data(obj, info):
            data = obj["data"]
            blob_base64 = base64.b64encode(data["blob"]).decode("utf-8")
            return blob_base64

        @spectrum.field("metadata")
        def resolve_metadata(obj, info):
            return obj["metadata"]

        @spectrum_metadata.field("name")
        def resolve_name(obj, info):
            return obj["name"]

        @spectrum_metadata.field("symbol")
        def resolve_symbol(obj, info):
            return obj["common"]["element"]["symbol"]

        @spectrum_metadata.field("edge")
        def resolve_edge(obj, info):
            return obj["common"]["element"]["edge"]

        # TODO can this be extended or does it have to be constructed at the beginning
        # define graphql endpoint to do approximately the same thing as /node/search
        schema = make_executable_schema(type_defs, query, spectrum, spectrum_metadata)
        graphql = GraphQL(schema, debug=True)

        router = APIRouter()

        # FIXME how to deal with multiple trees
        # TODO implement metadata search through the graphql endpoint
        @router.get("/graphql")
        async def graphiql(request: Request, user: str = Depends(get_current_user)):
            #FIXME how should authorization work from the playground app?
            #use {"x-tiled-api-key" : "key"} in HTTP Headers section of interface
            return await graphql.render_playground(request=request)

        @router.post("/graphql")
        async def graphql_post(request: Request, user: str = Depends(get_current_user)):
            return await graphql.graphql_http_server(request=request)

        self.include_routers = [router]

    def _build_dataset(self, doc):
        data = doc["content"]["data"]
        assert data["structure_family"] == "dataframe"
        assert data["media_type"] == "application/x-parquet"
        df = deserialize_parquet(data["blob"])
        metadata = doc["content"]["metadata"]
        return DataFrameAdapter.from_pandas(df, metadata=metadata, npartitions=1)

    def _build_node(self, doc):
        if doc["leaf"]:
            return self._build_dataset(doc)
        else:
            return self.new_variation(parent=doc["_id"])


def run_raw_mongo_query(query, tree):
    return tree.new_variation(
        queries=tree._queries + [json.loads(query.query)],
    )

def run_element_query(query, tree):
    results = tree.query_registry(
            RawMongoQuery({"metadata.common.element.symbol" : query.symbol, "metadata.common.element.edge" : query.edge}),
            tree)
    return results

MongoXASTree.register_query(RawMongoQuery, run_raw_mongo_query)
MongoXASTree.register_query(ElementQuery, run_element_query)
