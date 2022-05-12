import collections.abc
import copy
import io
import json
from dataclasses import dataclass
from enum import Enum

import h5py
import pymongo
from tiled.adapters.utils import IndexersMixin
from tiled.query_registration import QueryTranslationRegistry, register
from tiled.utils import UNCHANGED, DictView, ListView

from aimmdb.adapters.xas import XASAdapter
from aimmdb.serialization import deserialize_parquet


class OperationEnum(str, Enum):
    distinct = "distinct"
    lookup = "lookup"
    keys = "keys"


# TODO distinct operation should be able to inject metadata
def parse_path(path):
    key_translation = {
        "uid": "_id",
        "element": "metadata.element.symbol",
        "edge": "metadata.element.edge",
        "sample": "metadata.sample._id",
        "dataset": "metadata.dataset",
    }
    valid_keys = set(key_translation.keys())
    keys = path[0::2]
    values = path[1::2]

    if not set(keys).issubset(valid_keys):
        invalid_keys = set(keys) - valid_keys
        raise KeyError(f"keys {invalid_keys} not in {valid_keys}")

    select = {key_translation[k]: v for k, v in zip(keys, values)}
    leftover_keys = valid_keys - set(keys)

    # if we have more keys then values then get distinct values for the last key
    if len(keys) == len(values) + 1:
        operation = (
            OperationEnum("distinct"),
            {"select": select, "distinct": key_translation[keys[-1]]},
        )
    # if keys and values are matched then perform a lookup if uid was provided otherwise get remaining keys
    elif len(keys) == len(values):
        if "uid" in keys:
            operation = (OperationEnum("lookup"), {"select": select})
        else:
            operation = (
                OperationEnum("keys"),
                {"keys": leftover_keys, "select": select},
            )
    else:
        raise KeyError(f"{len(keys)=}, {len(values)=}")

    return operation


# @register(name="raw_mongo")
# @dataclass
# class RawMongoQuery:
#    """
#    Run a MongoDB query against a given collection.
#    """
#
#    query: str  # We cannot put a dict in a URL, so this a JSON str.
#
#    def __init__(self, query):
#        if isinstance(query, collections.abc.Mapping):
#            query = json.dumps(query)
#        self.query = query


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

    from aimmdb.graphql import GQLRouter
    from aimmdb.router_tiled import router

    router.include_router(GQLRouter, prefix="/graphql")
    include_routers = [router]

    specs = ["AIMMCatalog"]

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
        path=None,
    ):
        self._db = db

        if metadata:
            self._metadata = copy.deepcopy(metadata)
        else:
            self._metadata = {}
            self._metadata["sample"] = {}
            self._metadata["element"] = {}
            self._metadata["_tiled"] = {}

        self._access_policy = access_policy
        self._principal = principal

        self._queries = list(queries or [])

        self._path = list(path or [])

        self._op = parse_path(self.path)

        self._metadata["_tiled"]["op"] = self._op[0].value

        # if we have performed a lookup on samples inject the sample metadata
        if "metadata.sample._id" in self._op[1]["select"]:
            sample_id = self._op[1]["select"]["metadata.sample._id"]
            sample = self.db.samples.find_one({"_id": sample_id})
            if sample:
                self._metadata["sample"].update(sample)

        if "metadata.element.symbol" in self._op[1]["select"]:
            symbol = self._op[1]["select"]["metadata.element.symbol"]
            self._metadata["element"]["symbol"] = symbol

        if "metadata.element.edge" in self._op[1]["select"]:
            edge = self._op[1]["select"]["metadata.element.edge"]
            self._metadata["element"]["edge"] = edge

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

    @property
    def path(self):
        return ListView(self._path)

    @property
    def op(self):
        return self._op

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
        path=UNCHANGED,
    ):
        if principal is UNCHANGED:
            principal = self.principal
        if queries is UNCHANGED:
            queries = self.queries
        if metadata is UNCHANGED:
            # NOTE we want to pass underlying dict instead of DictView so that it can be modified in __init__
            metadata = self._metadata
        if path is UNCHANGED:
            path = self.path

        return type(self)(
            db=self._db,
            metadata=metadata,
            access_policy=self._access_policy,
            principal=principal,
            queries=queries,
            path=path,
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
            metadata = doc["metadata"]
            metadata.update(uid=doc["_id"])
            return XASAdapter.from_pandas(df, metadata=metadata, npartitions=1)
        else:
            raise RuntimeError(f"unhandled structure family {structf}")

    def _build_mongo_query(self, *queries):
        combined = self._queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def __getitem__(self, key):
        path = self.path + [key]
        operation = parse_path(path)

        # if new path is a lookup, do it now
        if operation[0] == OperationEnum.lookup:
            select = operation[1]["select"]
            if "_id" not in select:
                raise RuntimeError(f"_id not in {select}")

            query = self._build_mongo_query(select)

            docs = list(self.db.measurements.find(query))

            if len(docs) == 0:
                raise KeyError(f"{key} not found")

            if len(docs) > 1:
                raise KeyError(f"{key} matched multipled records")

            doc = docs[0]

            return self._build_node(doc)

        else:
            return self.new_variation(path=path)

    def __len__(self):
        if self.op[0] == OperationEnum.keys:
            return len(self.op[1]["keys"])
        elif self.op[0] == OperationEnum.distinct:
            select = self.op[1]["select"]
            distinct = self.op[1]["distinct"]
            query = self._build_mongo_query(select)
            # NOTE _id is guarenteed unique
            if distinct == "_id":
                return self.db.measurements.count_documents(query)
            else:
                # FIXME wasteful to do the full self.op just to get the length
                return len(self.db.measurements.find(query).distinct(distinct))
        elif self.op[0] == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    def __iter__(self):
        if self.op[0] == OperationEnum.keys:
            yield from self.op[1]["keys"]
        elif self.op[0] == OperationEnum.distinct:
            select = self.op[1]["select"]
            distinct = self.op[1]["distinct"]
            query = self._build_mongo_query(select)
            # NOTE _id is guarenteed unique
            if distinct == "_id":
                for doc in self.db.measurements.find(query, {"_id": 1}):
                    yield doc["_id"]
            else:
                for v in self.db.measurements.find(query).distinct(distinct):
                    yield v
        elif self.op[0] == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.
    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        if self.op[0] == OperationEnum.keys:
            yield from list(self.op[1]["keys"])[skip : skip + limit]
        elif self.op[0] == OperationEnum.distinct:
            select = self.op[1]["select"]
            distinct = self.op[1]["distinct"]
            query = self._build_mongo_query(select)
            # NOTE _id is guarenteed unique
            if distinct == "_id":
                for doc in (
                    self.db.measurements.find(query, {"_id": 1}).skip(skip).limit(limit)
                ):
                    yield doc["_id"]
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.db.measurements.find(query).distinct(distinct)[
                    skip : skip + limit
                ]:
                    yield v
        elif self.op[0] == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        if self.op[0] == OperationEnum.keys:
            yield from [
                (k, self[k]) for k in list(self.op[1]["keys"])[skip : skip + limit]
            ]
        elif self.op[0] == OperationEnum.distinct:
            select = self.op[1]["select"]
            distinct = self.op[1]["distinct"]
            query = self._build_mongo_query(select)
            # NOTE _id is guarenteed unique
            if distinct == "_id":
                for doc in (
                    self.db.measurements.find(query, {"_id": 1}).skip(skip).limit(limit)
                ):
                    k = doc["_id"]
                    yield (k, self[k])
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.db.measurements.find(query).distinct(distinct)[
                    skip : skip + limit
                ]:
                    yield (v, self[v])
        elif self.op[0] == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        return self._items_slice(index, index + 1, 1)

    def read(self, fields=None):
        if fields is not None:
            raise NotImplementedError
        return self


# def run_raw_mongo_query(query, tree):
#    query = json.loads(query.query)
#    return tree.new_variation(queries=tree._queries + [query])
#
#
# AIMMTree.register_query(RawMongoQuery, run_raw_mongo_query)


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
    elif isinstance(node, XASAdapter):
        df = node.read()
        yield from walk({k: df[k].to_numpy() for k in df}, pre + ["data"])
        if node.metadata:
            yield from walk(node.metadata, pre + ["metadata"])
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
