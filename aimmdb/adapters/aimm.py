import collections.abc
import json
import os
from pathlib import Path

import pymongo
import pydantic

from tiled.adapters.utils import IndexersMixin, tree_repr
from tiled.query_registration import QueryTranslationRegistry
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import serialize_arrow
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED, DictView, ListView

import aimmdb.uid
from aimmdb.adapters.array import WritingArrayAdapter
from aimmdb.adapters.dataframe import WritingDataFrameAdapter
from aimmdb.queries import RawMongo, OperationEnum, parse_path
from aimmdb.schemas import GenericDocument, XASMetadata


_mime_structure_association = {
    StructureFamily.array: "application/x-hdf5",
    StructureFamily.dataframe: APACHE_ARROW_FILE_MIME_TYPE,
}

key_translation = {
    "uid": "uid",
    "element": "metadata.element.symbol",
    "edge": "metadata.element.edge",
}


class Document(GenericDocument[XASMetadata]):
    @pydantic.validator("specs")
    def check_specs(cls, v):
        assert "XAS" in v
        return v


class AIMMCatalog(collections.abc.Mapping, IndexersMixin):
    structure_family = "node"
    specs = ["AIMMCatalog"]

    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    # TODO remove when writing routes are upstreamed to tiled
    from aimmdb.router_tiled import router

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
        path=None,
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
        self.samples_collection = metadata_db.get_collection("samples")

        self.queries = queries or []
        self.sorting = sorting or []
        self.metadata = metadata or {}
        self.principal = principal
        self.access_policy = access_policy

        self.path = list(path or [])
        self.op = parse_path(self.path, key_translation)

        super().__init__()

    @classmethod
    def from_uri(
        cls,
        uri,
        data_directory,
        *,
        metadata=None,
        access_policy=None,
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
        )

    @classmethod
    def from_mongomock(cls, data_directory, *, metadata=None):
        import mongomock

        mongo_client = mongomock.MongoClient()
        metadata_db = mongo_client["test"]

        return cls(
            metadata_db=metadata_db,
            data_directory=data_directory,
            metadata=metadata,
        )

    def authenticated_as(self, principal):
        if self.principal is not None:
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
        path=UNCHANGED,
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
        if path is UNCHANGED:
            path = self.path
        return type(self)(
            metadata_db=self.metadata_db,
            data_directory=self.data_directory,
            metadata=metadata,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            principal=principal,
            path=path,
            **kwargs,
        )

    def search(self, query):
        """
        Return a AIMMCatalog with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def post_metadata(self, metadata, structure_family, structure, specs):
        if self.path != ["uid"]:
            raise RuntimeError("AIMMCatalog only allows posting data to /uid")

        uid = aimmdb.uid.uid()

        # FIXME how to validate/enforce specs
        validated_document = Document(
            uid=uid,
            structure_family=structure_family,
            structure=structure,
            metadata=metadata,
            specs=specs,
            mimetype=_mime_structure_association[structure_family],
        )

        # After validating the document must be encoded to bytes again to make it compatible with MongoDB
        if validated_document.structure_family == StructureFamily.dataframe:
            validated_document.structure.micro.meta = bytes(
                serialize_arrow(validated_document.structure.micro.meta, {})
            )

        self.metadata_collection.insert_one(validated_document.dict())
        return uid

    def _build_node_from_doc(self, doc):
        if doc["structure_family"] == StructureFamily.array:
            return WritingArrayAdapter(
                self.metadata_collection, self.data_directory, Document.parse_obj(doc)
            )
        elif doc["structure_family"] == StructureFamily.dataframe:
            return WritingDataFrameAdapter(
                self.metadata_collection, self.data_directory, Document.parse_obj(doc)
            )
        else:
            raise ValueError("Unsupported Structure Family value in the databse")

    def _build_mongo_query(self, *queries):
        combined = self.queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def __getitem__(self, key):
        path = self.path + [key]
        operation = parse_path(path, key_translation)

        # if new path is a lookup, do it now
        if operation[0] == OperationEnum.lookup:
            select = operation[1]["select"]
            if "uid" not in select:
                raise RuntimeError(f"uid not in {select}")

            query = self._build_mongo_query(select)

            docs = list(self.metadata_collection.find(query))

            if len(docs) == 0:
                raise KeyError(f"{key} not found")

            if len(docs) > 1:
                raise KeyError(f"{key} matched multipled records")

            doc = docs[0]

            return self._build_node_from_doc(doc)

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
            if distinct == "uid":
                return self.metadata_collection.count_documents(query)
            else:
                # FIXME wasteful to do the full self.op just to get the length
                return len(self.metadata_collection.find(query).distinct(distinct))
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
            if distinct == "uid":
                for doc in self.metadata_collection.find(query, {"uid": 1}):
                    yield doc["uid"]
            else:
                for v in self.metadata_collection.find(query).distinct(distinct):
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
            if distinct == "uid":
                for doc in (
                    self.metadata_collection.find(query, {"uid": 1})
                    .skip(skip)
                    .limit(limit)
                ):
                    yield doc["uid"]
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.metadata_collection.find(query).distinct(distinct)[
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
            if distinct == "uid":
                for doc in (
                    self.metadata_collection.find(query, {"uid": 1})
                    .skip(skip)
                    .limit(limit)
                ):
                    k = doc["uid"]
                    yield (k, self[k])
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.metadata_collection.find(query).distinct(distinct)[
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


def run_raw_mongo_query(query, tree):
    query = json.loads(query.query)
    return tree.new_variation(queries=tree.queries + [query])


AIMMCatalog.register_query(RawMongo, run_raw_mongo_query)
