import collections.abc
import io
import json
from dataclasses import dataclass
import typing
from typing import List, Optional

import fastapi
from fastapi import APIRouter
from fastapi import Depends, Security

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.scalars import JSON
from strawberry.types import Info
from strawberry.tools import create_type, merge_types
from strawberry.permission import BasePermission

import h5py
import pymongo
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.utils import IndexersMixin
from tiled.query_registration import QueryTranslationRegistry, register
from tiled.utils import UNCHANGED, DictView, import_object, SpecialUsers

from tiled.server.authentication import get_current_principal
from tiled.server.dependencies import get_root_tree

from .serialization import deserialize_parquet
from .authentication import AIMMAuthenticator


class WritePermission(BasePermission):
    message = "User does not have write permission"

    async def has_permission(self, source: typing.Any, info: Info, **kwargs) -> bool:
        try:
            principal = info.context["principal"]
            root = info.context["root_tree"]
            return root.access_policy.has_write_permission(principal)
        except Exception as e:
            print(f"Error while checking WritePermission: {e}")
            return False


class ReadPermission(BasePermission):
    message = "User does not have read permission"

    async def has_permission(self, source: typing.Any, info: Info, **kwargs) -> bool:
        try:
            principal = info.context["principal"]
            root = info.context["root_tree"]
            return root.access_policy.has_read_permission(principal)
        except Exception as e:
            print(f"Error while checking ReadPermission: {e}")
            return False


async def get_context(
    principal=Security(get_current_principal, scopes=["read:metadata"]),
    root_tree=Depends(get_root_tree),
):
    return {"principal": principal, "root_tree": root_tree}


@strawberry.field(permission_classes=[ReadPermission])
def hello(info: Info) -> str:
    principal = info.context["principal"]
    return f"Hello {principal}"


@strawberry.mutation(permission_classes=[WritePermission])
def record_message(message: str, info: Info) -> Optional[str]:
    root = info.context["root_tree"]
    db = root.db
    try:
        result = db.messages.insert_one({"message": message})
        return str(result.inserted_id)
    except RuntimeError as e:
        print(e)
        return None


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
        tree_collection_name,
        data_collection_name,
        *,
        metadata=None,
        access_policy=None,
        principal=None,
    ):

        db = _get_database(uri, username, password)
        tree = db.get_collection(tree_collection_name)
        data = db.get_collection(data_collection_name)

        return cls(
            db,
            tree,
            data,
            metadata=metadata,
            access_policy=access_policy,
            principal=principal,
        )

    def __init__(
        self,
        db,
        tree,
        data,
        metadata=None,
        access_policy=None,
        principal=None,
        queries=None,
        path="/",
    ):
        self._db = db

        self._tree = tree
        self._data = data

        self._metadata = metadata or {}

        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Adapter."
            )
        self._access_policy = access_policy
        self._principal = principal

        self._queries = list(queries or [])
        self._path = path

        GQLQuery = create_type("Query", [hello])
        GQLMutation = create_type("Mutation", [record_message])

        GQLSchema = strawberry.Schema(query=GQLQuery, mutation=GQLMutation)
        GQLRouter = GraphQLRouter(GQLSchema, context_getter=get_context, graphiql=False)

        router = APIRouter()
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
    def data_collection(self):
        return self._data

    @property
    def tree_collection(self):
        return self._tree

    @property
    def principal(self):
        return self._principal

    @property
    def path(self):
        return self._path

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

    def _build_mongo_query(self, *queries):
        combined = [{"path": {"$regex": f"^{self._path}[^/]*$"}}]
        combined += self._queries
        combined += list(queries)
        if combined:
            query = {"$and": combined}
        else:
            query = {}
        return query

    def _build_node(self, doc):
        if doc["structure_family"] == "node":
            name = doc["name"]
            metadata = doc["metadata"]
            return self.new_variation(path=f"{self._path}{name}/", metadata=metadata)
        elif doc["structure_family"] == "dataframe":
            data_doc = self._data.find_one({"_id": doc["data_id"]})
            assert data_doc["structure_family"] == "dataframe"
            df = deserialize_parquet(data_doc["data"]["blob"])
            return DataFrameAdapter.from_pandas(
                df, metadata=data_doc["metadata"], npartitions=1
            )

    def __len__(self):
        return self._tree.count_documents(self._build_mongo_query())

    def __getitem__(self, key):
        query = self._build_mongo_query({"name": key})
        docs = list(self._tree.find(query))

        if len(docs) == 0:
            raise KeyError(f"{key} not found")

        if len(docs) > 1:
            raise KeyError(f"{key} matched multipled records")

        return self._build_node(docs[0])

    def __iter__(self):
        query = self._build_mongo_query()
        for doc in self._tree.find(query, {"name": 1}):
            yield str(doc["name"])

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
        path=UNCHANGED,
        metadata=UNCHANGED,
    ):
        if principal is UNCHANGED:
            principal = self.principal
        if queries is UNCHANGED:
            queries = self.queries
        if path is UNCHANGED:
            path = self.path
        if metadata is UNCHANGED:
            metadata = self.metadata

        return type(self)(
            db=self._db,
            tree=self._tree,
            data=self._data,
            metadata=metadata,
            access_policy=self._access_policy,
            principal=principal,
            queries=queries,
            path=path,
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

        query = self._build_mongo_query()
        for doc in self._tree.find(query, {"name": 1}).skip(skip).limit(limit):
            k = str(doc["name"])
            yield k

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self._tree.find(self._build_mongo_query()).skip(skip).limit(limit):
            k = str(doc["name"])
            dset = self._build_node(doc)
            yield (k, dset)

    def _item_by_index(self, index, direction):
        assert direction == 1, "direction=-1 should be handled by the client"

        doc = next(self._tree.find(self._build_mongo_query()).skip(index).limit(1))
        k = str(doc["name"])
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


class AIMMAccessPolicy:
    READ = object()  # sentinel
    READWRITE = object()  # sentinel

    def __init__(self, access_lists, *, provider):
        self.access_lists = {}
        self.provider = provider
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            if not value in (self.READ, self.READWRITE):
                raise KeyError(
                    f"AIMMAccessPolicy: value {value} is not AIMMAccessPolicy.READ or AIMMAcccessPolicy.READWRITE"
                )
            self.access_lists[key] = value

    def check_compatibility(self, tree):
        return isinstance(tree, AIMMTree)

    def get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.
        for identity in principal.identities:
            if identity.provider == self.provider:
                return identity.id
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )

    def has_read_permission(self, principal):
        id = self.get_id(principal)
        return (principal is SpecialUsers.admin) or (id in self.access_lists.keys())

    def has_write_permission(self, principal):
        id = self.get_id(principal)
        permission = self.access_lists.get(id, None)
        return (principal is SpecialUsers.admin) or (permission is self.READWRITE)

    def filter_results(self, tree, principal):
        if self.has_read_permission(principal):
            return tree
        else:
            return MapAdapter({})
