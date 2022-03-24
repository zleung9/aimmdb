import collections.abc
import json
from dataclasses import dataclass

import pymongo
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.utils import IndexersMixin
from tiled.query_registration import QueryTranslationRegistry, register
from tiled.utils import UNCHANGED, DictView

from .serialization import deserialize_parquet


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


class AIMMTree(collections.abc.Mapping, IndexersMixin):

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
        *,
        metadata=None,
        access_policy=None,
        principal=None,
    ):

        db = _get_database(uri, username, password)
        required_collections = ["samples", "measurements"]
        if not set(required_collections).issubset(set(db.list_collection_names())):
            print("initializing mongodb")
            # setup indexes here
            db.create_collection("samples")
            db.create_collection("measurements")

        return cls(
            db,
            metadata=metadata,
            access_policy=access_policy,
            principal=principal,
        )

    def __init__(
        self,
        db,
        metadata=None,
        access_policy=None,
        principal=None,
        queries=None,
        init_db=False,
    ):
        self._db = db

        self._metadata = metadata or {}

        self._access_policy = access_policy
        self._principal = principal

        self._queries = list(queries or [])

        from .router import router
        from .graphql import GQLRouter

        router.include_router(GQLRouter, prefix="/graphql")
        self.include_routers = [router]

        super().__init__()

    @property
    def access_policy(self):
        return self._access_policy

    @access_policy.setter
    def access_policy(self, value):
        self._access_policy = value

    @property
    def db(self):
        return self._db

    @property
    def principal(self):
        return self._principal

    @property
    def queries(self):
        return DictView(self._queries)

    @property
    def metadata(self):
        "Metadata about this Tree."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def authenticated_as(self, principal):
        if self._principal is not None:
            raise RuntimeError(f"Already authenticated as {self.principal}")
        if self._access_policy is not None:
            tree = self._access_policy.filter_results(self, principal)
        else:
            tree = self.new_variation(principal=principal)
        return tree

    def new_variation(
        self,
        principal=UNCHANGED,
        queries=UNCHANGED,
        metadata=UNCHANGED,
    ):
        if principal is UNCHANGED:
            principal = self.principal
        if queries is UNCHANGED:
            queries = self.queries
        if metadata is UNCHANGED:
            metadata = self.metadata

        return type(self)(
            db=self._db,
            metadata=metadata,
            access_policy=self._access_policy,
            principal=principal,
            queries=queries,
        )

    def search(self, query):
        """
        Return a Tree with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def _build_node(self, doc):
        structf = doc["structure_family"]
        if structf == "dataframe":
            df = deserialize_parquet(doc["data"]["blob"])
            return DataFrameAdapter.from_pandas(
                df, metadata=doc["metadata"], npartitions=1
            )
        else:
            raise RuntimeError(f"unhandled structure family {structf}")

    def _build_mongo_query(self, *queries):
        combined = self._queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def __len__(self):
        query = self._build_mongo_query()
        return self.db.measurements.count_documents(query)

    def __getitem__(self, key):
        docs = list(self.db.measurements.find({"_id": key}))

        if len(docs) == 0:
            raise KeyError(f"{key} not found")

        if len(docs) > 1:
            raise KeyError(f"{key} matched multipled records")

        doc = docs[0]

        return self._build_node(doc)

    def __iter__(self):
        query = self._build_mongo_query()
        for doc in self.db.datasets.find(query, {"_id": 1}):
            yield str(doc["_id"])

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.
    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        query = self._build_mongo_query()
        for doc in self.db.measurements.find(query, {"_id": 1}).skip(skip).limit(limit):
            k = str(doc["_id"])
            yield k

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        query = self._build_mongo_query()
        for doc in self.db.measurements.find(query).skip(skip).limit(limit):
            k = str(doc["_id"])
            dset = self._build_node(doc)
            yield (k, dset)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"

        query = self._build_mongo_query()
        doc = next(self.db.measurements.find(query).skip(index).limit(1))
        k = str(doc["_id"])
        dset = self._build_node(doc)
        return (k, dset)

    def read(self, fields=None):
        if fields is not None:
            raise NotImplementedError
        return self


def run_raw_mongo_query(query, tree):
    query = json.loads(query.query)
    return tree.new_variation(queries=tree._queries + [query])


AIMMTree.register_query(RawMongoQuery, run_raw_mongo_query)


def walk(node, pre=None):
    pre = pre[:] if pre else []

    if isinstance(node, AIMMTree):
        for k, v in node.items():
            yield from walk(v, pre + [k])
        if node.metadata:
            yield from walk(node.metadata, pre + ["metadata"])
    elif isinstance(node, collections.abc.Mapping):
        for k, v in node.items():
            yield from walk(v, pre + [k])
    elif isinstance(node, DataFrameAdapter):
        df = node.read()
        yield from walk({k: df[k].to_numpy() for k in df}, pre + ["data"])
        if node.metadata:
            yield from walk(node.metadata, pre + ["metadata"])
    else:
        yield (node, pre)
