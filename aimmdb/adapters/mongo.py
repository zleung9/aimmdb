import collections.abc
import dataclasses
import json
import os
from collections import defaultdict
from pathlib import Path

import fastapi
import pydantic
import pymongo
from fastapi import HTTPException
from pydantic import ValidationError
from tiled.adapters.utils import IndexersMixin, tree_repr
from tiled.iterviews import ItemsView, KeysView, ValuesView
from tiled.query_registration import QueryTranslationRegistry
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import serialize_arrow
from tiled.utils import (
    APACHE_ARROW_FILE_MIME_TYPE,
    UNCHANGED,
    DictView,
    ListView,
    import_object,
)

import aimmdb.uid
from aimmdb.access import READ, WRITE, require_write_permission
from aimmdb.adapters.array import WritingArrayAdapter
from aimmdb.adapters.dataframe import WritingDataFrameAdapter
from aimmdb.queries import RawMongo
from aimmdb.schemas import GenericDocument
from aimmdb.utils import make_dict

_mime_structure_association = {
    StructureFamily.array: "application/x-hdf5",
    StructureFamily.dataframe: APACHE_ARROW_FILE_MIME_TYPE,
}


class Metadata(pydantic.BaseModel, extra=pydantic.Extra.allow):
    pass


Document = GenericDocument[Metadata]


class MongoAdapter(collections.abc.Mapping):
    structure_family = "node"
    specs = ["MongoAdapter"]

    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    # TODO remove when writing routes are upstreamed to tiled
    from aimmdb.server.router_tiled import router

    include_routers = [router]

    def __init__(
        self,
        *,
        metadata_db,
        data_directory,
        queries=None,
        sorting=None,
        metadata=None,
        principal=None,
        access_policy=None,
        spec_to_document_model=None,
    ):
        self.data_directory = Path(data_directory).resolve()
        if not self.data_directory.exists():
            raise ValueError(f"Directory {self.data_directory} does not exist.")
        if not self.data_directory.is_dir():
            raise ValueError(
                f"The given directory path {self.data_directory} is not a directory."
            )
        if not os.access(self.data_directory, os.W_OK):
            raise ValueError("Directory {self.directory} is not writeable.")

        self.metadata_db = metadata_db
        self.metadata_collection = metadata_db.get_collection("metadata")

        self.queries = queries or []
        self.sorting = sorting or []
        self.metadata = metadata or {}
        self.principal = principal
        self.access_policy = access_policy

        if spec_to_document_model is None:
            self.spec_to_document_model = defaultdict(lambda: Document)
        else:
            default_document_model = spec_to_document_model.pop("default", Document)
            self.spec_to_document_model = defaultdict(
                lambda: default_document_model,
                {k: import_object(v) for k, v in spec_to_document_model.items()},
            )

        super().__init__()

    @classmethod
    def from_uri(
        cls,
        uri,
        data_directory,
        *,
        metadata=None,
        access_policy=None,
        spec_to_document_model=None,
    ):
        if not pymongo.uri_parser.parse_uri(uri)["database"]:
            raise ValueError(
                f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
            )
        metadata_db = pymongo.MongoClient(uri).get_database()

        return cls(
            metadata_db=metadata_db,
            data_directory=data_directory,
            metadata=metadata,
            access_policy=access_policy,
            spec_to_document_model=spec_to_document_model,
        )

    @classmethod
    def from_mongomock(
        cls,
        data_directory,
        *,
        metadata=None,
        access_policy=None,
        spec_to_document_model=None,
    ):
        import mongomock

        mongo_client = mongomock.MongoClient()
        metadata_db = mongo_client["test"]

        return cls(
            metadata_db=metadata_db,
            data_directory=data_directory,
            metadata=metadata,
            access_policy=access_policy,
            spec_to_document_model=spec_to_document_model,
        )

    @property
    def permissions(self):
        """
        Return the permissions of the current principal
        """
        if self.access_policy is not None:
            permissions = self.access_policy.permissions(self.principal)
        else:
            # no access_policy => anyone can read/write
            permissions = {READ, WRITE}

        # we should never reach a node which principal does not have permission to read
        if READ not in permissions:
            raise RuntimeError("reached unreadable node")

        return permissions

    def authenticated_as(self, principal):
        if self.principal is not None and self.principal != principal:
            raise RuntimeError(f"Already authenticated as {self.principal}")

        if self.access_policy is not None:
            tree = self.access_policy.filter_results(self, principal)
        else:
            tree = self.new_variation(principal=principal)
        return tree

    def new_variation(
        self,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        principal=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self.metadata
        if queries is UNCHANGED:
            queries = self.queries
        if sorting is UNCHANGED:
            sorting = self.sorting
        if principal is UNCHANGED:
            principal = self.principal
        return type(self)(
            metadata_db=self.metadata_db,
            data_directory=self.data_directory,
            metadata=metadata,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            principal=principal,
            spec_to_document_model=self.spec_to_document_model,
            **kwargs,
        )

    def search(self, query):
        """
        Return a MongoAdapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def _get_document_model(self, specs):
        spec_to_document_model_keys = set(
            self.spec_to_document_model.keys()
        ).intersection(specs)
        if len(spec_to_document_model_keys) > 1:
            raise KeyError(f"specs {specs} matched more than one document model")
        k = spec_to_document_model_keys.pop() if spec_to_document_model_keys else None
        document_model = self.spec_to_document_model[k]
        return document_model

    @require_write_permission
    def post_metadata(self, metadata, structure_family, structure, specs):
        key = aimmdb.uid.uid()

        try:
            document_model = self._get_document_model(specs)
        except KeyError as err:
            raise HTTPException(status_code=400, detail=f"{err}")

        try:
            validated_document = document_model(
                uid=key,
                structure_family=structure_family,
                structure=structure,
                metadata=metadata,
                specs=specs,
                mimetype=_mime_structure_association[structure_family],
            )
        except pydantic.ValidationError as err:
            raise HTTPException(status_code=400, detail=f"{err}")

        self.metadata_collection.insert_one(validated_document.dict())
        return key

    def _build_node_from_doc(self, doc):
        # NOTE we don't use self._get_document_model to do extra validation based on specs
        document_model = Document

        if doc["structure_family"] == StructureFamily.array:
            return WritingArrayAdapter(
                self.metadata_collection,
                self.data_directory,
                document_model.parse_obj(doc),
                self.permissions,
            )
        elif doc["structure_family"] == StructureFamily.dataframe:
            return WritingDataFrameAdapter(
                self.metadata_collection,
                self.data_directory,
                document_model.parse_obj(doc),
                self.permissions,
            )
        else:
            raise ValueError("Unsupported Structure Family value in the databse")

    def _build_mongo_query(self, *queries):
        combined = self.queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def __len__(self):
        count = self.metadata_collection.count_documents(
            # self._build_mongo_query({"active": True})
            self._build_mongo_query({"data_url": {"$ne": None}})
        )
        return count

    def __length_hint__(self):
        # https://www.python.org/dev/peps/pep-0424/
        return self.metadata_collection.estimated_document_count(
            # self._build_mongo_query({"active": True}),
            self._build_mongo_query({"data_url": {"$ne": None}}),
        )

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return tree_repr(self, self._keys_slice(0, N, direction=1))

    def __getitem__(self, key):
        query = {"uid": key}
        doc = self.metadata_collection.find_one(self._build_mongo_query(query))
        if doc is None:
            raise KeyError(key)

        return self._build_node_from_doc(doc)

    def __iter__(self):
        # TODO Apply pagination, as we do in Databroker.
        for doc in list(
            self.metadata_collection.find(
                # self._build_mongo_query({"active": True}), {"uid": True}
                self._build_mongo_query({"data_url": {"$ne": None}}),
                {"_id": False},
            )
        ):
            yield doc["uid"]

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for doc in self.metadata_collection.find(
            # self._build_mongo_query({"active": True}),
            self._build_mongo_query({"data_url": {"$ne": None}}),
            skip=skip,
            limit=limit,
        ):
            yield doc["uid"]

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        for doc in self.metadata_collection.find(
            # self._build_mongo_query({"active": True}),
            self._build_mongo_query({"data_url": {"$ne": None}}),
            skip=skip,
            limit=limit,
        ):
            yield (doc["uid"], self._build_node_from_doc(doc))

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)


def run_raw_mongo_query(query, tree):
    query = json.loads(query.query)
    return tree.new_variation(queries=tree.queries + [query])


MongoAdapter.register_query(RawMongo, run_raw_mongo_query)
