import collections.abc
import copy
import json
import os
from collections import defaultdict
from pathlib import Path

import pydantic
import pymongo
from fastapi import HTTPException
from tiled.adapters.utils import IndexersMixin, tree_repr
from tiled.iterviews import ItemsView, KeysView, ValuesView
from tiled.query_registration import QueryTranslationRegistry
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import serialize_arrow
from tiled.utils import (APACHE_ARROW_FILE_MIME_TYPE, UNCHANGED, DictView,
                         ListView, import_object)

import aimmdb.queries
import aimmdb.uid
from aimmdb.access import READ, WRITE
from aimmdb.adapters.array import WritingArrayAdapter
from aimmdb.adapters.dataframe import WritingDataFrameAdapter
from aimmdb.queries import OperationEnum, RawMongo, parse_path
from aimmdb.schemas import GenericDocument
from aimmdb.utils import make_dict

_mime_structure_association = {
    StructureFamily.array: "application/x-hdf5",
    StructureFamily.dataframe: APACHE_ARROW_FILE_MIME_TYPE,
}

key_to_query = {
    "uid": "uid",
    "element": "metadata.element.symbol",
    "edge": "metadata.element.edge",
    "dataset": "metadata.dataset",
    "sample": "metadata.sample_id",
}

# default document model requires dataset in metadata
class MetadataBase(pydantic.BaseModel, extra=pydantic.Extra.allow):
    dataset: str


Document = GenericDocument[MetadataBase]


class AIMMCatalog(collections.abc.Mapping):
    structure_family = "node"
    specs = ["AIMMCatalog"]

    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    # TODO remove when writing routes are upstreamed to tiled
    from aimmdb.router import router
    from aimmdb.router_tiled import router as router_tiled

    include_routers = [router_tiled, router]

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
        spec_to_document_model=None,
        dataset_to_specs=None,
    ):

        # FIXME try to do less everytime we construct a new object

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
        self.sample_collection = metadata_db.get_collection("samples")

        self.queries = queries or []
        self.sorting = sorting or []
        self.metadata = copy.deepcopy(metadata) or {}
        self.principal = principal
        self.access_policy = access_policy

        self.path = list(path or [])
        self.op = parse_path(self.path, key_to_query)
        self.metadata["_tiled"] = {"op": self.op.dict()}

        # inject sample metadata if we have selected on sample
        self.metadata["_tiled"].pop("sample", None)
        sample_query = key_to_query["sample"]
        if sample_query in self.op.select:
            sample_id = self.op.select[sample_query]
            sample = self.sample_collection.find_one({"uid": sample_id}, {"_id": False})
            if sample is not None:
                self.metadata["_tiled"]["sample"] = sample

        if spec_to_document_model is None:
            self.spec_to_document_model = defaultdict(lambda: Document)
        else:
            default_document_model = spec_to_document_model.pop("default", Document)
            self.spec_to_document_model = defaultdict(
                lambda: default_document_model,
                {k: import_object(v) for k, v in spec_to_document_model.items()},
            )

        self.dataset_to_specs = dataset_to_specs or {}

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
        dataset_to_specs=None,
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
            dataset_to_specs=dataset_to_specs,
        )

    @classmethod
    def from_mongomock(
        cls,
        data_directory,
        *,
        metadata=None,
        access_policy=None,
        spec_to_document_model=None,
        dataset_to_specs=None,
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
            dataset_to_specs=dataset_to_specs,
        )

    def permissions(self, dataset):
        """
        Return the permissions of the current principal
        """
        # FIXME in more sophisticated cases permissions may DEPEND on metadata
        # For example only being able to write to a particular dataset
        if self.access_policy is not None:
            permissions = self.access_policy.permissions(self.principal, dataset)
        else:
            # no access_policy => anyone can read/write
            permissions = {READ, WRITE}

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
            spec_to_document_model=self.spec_to_document_model,
            dataset_to_specs=self.dataset_to_specs,
            **kwargs,
        )

    def search(self, query):
        """
        Return a AIMMCatalog with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def _get_document_model(self, specs):
        # FIXME think more about how to handle multiple specs
        spec_to_document_model_keys = set(
            self.spec_to_document_model.keys()
        ).intersection(specs)
        if len(spec_to_document_model_keys) > 1:
            raise KeyError(f"specs {specs} matched more than one document model")
        k = spec_to_document_model_keys.pop() if spec_to_document_model_keys else None
        document_model = self.spec_to_document_model[k]
        return document_model

    def post_sample(self, sample):
        # FIXME this is a bit adhoc (samples is not a 'real' dataset)
        dataset = "samples"
        permissions = self.permissions(dataset)
        if WRITE not in permissions:
            raise HTTPException(
                status_code=403,
                detail=f"principal does not have write permissions to dataset {dataset}",
            )

        sample.uid = aimmdb.uid.uid()
        result = self.sample_collection.insert_one(sample.dict())
        assert result.acknowledged == True
        return sample.uid

    def delete_sample(self, uid):
        # FIXME this is a bit adhoc (samples is not a 'real' dataset)
        dataset = "samples"
        permissions = self.permissions(dataset)
        if WRITE not in permissions:
            raise HTTPException(
                status_code=403,
                detail=f"principal does not have write permissions to dataset {dataset}",
            )

        # FIXME should we also delete measurements which refer to this sample?
        result = self.sample_collection.delete_one({"uid": uid})
        assert result.deleted_count == 1

    def post_metadata(self, metadata, structure_family, structure, specs):

        # TODO reconsider how this should work
        if self.path != ["uid"]:
            raise HTTPException(
                status_code=400, detail="AIMMCatalog only allows posting data to /uid"
            )

        # NOTE this is enforced outside of pydantic
        dataset = metadata.get("dataset")
        if dataset is None:
            raise HTTPException(
                status_code=400,
                detail="AIMMCatalog requires that metadata contain a dataset key",
            )

        permissions = self.permissions(dataset)
        if WRITE not in permissions:
            raise HTTPException(
                status_code=403,
                detail=f"principal does not have write permissions to dataset {dataset}",
            )

        key = aimmdb.uid.uid()

        try:
            document_model = self._get_document_model(specs)
        except KeyError as err:
            raise HTTPException(status_code=400, detail=f"{err}")

        # FIXME need to unpack into dict because DataFrameStructure is a dataclass so in this case structure will be
        # an anonymous pydantic generated type which will not pass validation for the document
        structure = make_dict(structure)

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

        # at this point according to the server the metadata matches the stated specs and the user has permissions to write
        # now check if we will accept these specs into the specified dataset
        allowed_specs = self.dataset_to_specs.get(dataset, None)

        # FIXME think more about how to handle multiple specs
        # if this dataset has a set of allowed specs then specs must be a non-empty subset
        if allowed_specs is not None:
            if not specs or not set(specs).issubset(allowed_specs):
                raise HTTPException(
                    status_code=400,
                    detail=f"specs ({specs}) are not a non-empty subset of allowed specs ({allowed_specs}) for dataset {dataset}",
                )

        # FIXME what if metadata is a dict?
        try:
            sample_id = validated_document.metadata.sample_id
        except AttributeError:
            sample_id = None

        doc_dict = make_dict(validated_document)

        if sample_id is not None:
            sample = self.sample_collection.find_one({"uid": sample_id}, {"_id": False})
            if sample is None:
                raise HTTPException(
                    status_code=400, detail=f"sample_id {sample_id} not found"
                )
            else:
                if "sample" in doc_dict["metadata"]:
                    doc_dict["metadata"]["sample"].update(sample)
                else:
                    doc_dict["metadata"]["sample"] = sample

        self.metadata_collection.insert_one(doc_dict)
        return key

    def _build_node_from_doc(self, doc):
        specs = doc.get("specs", [])
        document_model = self._get_document_model(specs)

        doc = document_model.parse_obj(doc)
        dataset = doc.metadata.dataset

        permissions = self.permissions(dataset)

        if doc.structure_family == StructureFamily.array:
            return WritingArrayAdapter(
                self.metadata_collection,
                self.data_directory,
                doc,
                permissions,
            )
        elif doc.structure_family == StructureFamily.dataframe:
            return WritingDataFrameAdapter(
                self.metadata_collection,
                self.data_directory,
                doc,
                permissions,
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
        op = parse_path(path, key_to_query)

        # if new path is a lookup, do it now
        if op.op_enum == OperationEnum.lookup:
            query = self._build_mongo_query(op.select)
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
        if self.op.op_enum == OperationEnum.keys:
            return len(self.op.keys)
        elif self.op.op_enum == OperationEnum.distinct:
            query = self._build_mongo_query(self.op.select)
            # NOTE _id is guarenteed unique
            if self.op.distinct == "uid":
                return self.metadata_collection.count_documents(query)
            else:
                # FIXME wasteful to do the full self.op just to get the length
                return len(
                    self.metadata_collection.find(query).distinct(self.op.distinct)
                )
        elif self.op.op_enum == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    def __iter__(self):
        if self.op.op_enum == OperationEnum.keys:
            yield from self.op.keys
        elif self.op.op_enum == OperationEnum.distinct:
            query = self._build_mongo_query(self.op.select)
            # NOTE uid is guarenteed unique
            if self.op.distinct == "uid":
                for doc in self.metadata_collection.find(query, {"uid": 1}):
                    yield doc["uid"]
            else:
                for v in self.metadata_collection.find(query).distinct(
                    self.op.distinct
                ):
                    yield v
        elif self.op[0] == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    # FIXME what do I need to do to make tail work
    # FIXME negative indexing is broken
    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None

        if self.op.op_enum == OperationEnum.keys:
            yield from list(self.op.keys)[skip : skip + limit]
        elif self.op.op_enum == OperationEnum.distinct:
            query = self._build_mongo_query(self.op.select)
            # NOTE uid is guarenteed unique
            if self.op.distinct == "uid":
                for doc in (
                    self.metadata_collection.find(query, {"uid": 1})
                    .skip(skip)
                    .limit(limit)
                ):
                    yield doc["uid"]
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.metadata_collection.find(query).distinct(
                    self.op.distinct
                )[skip : skip + limit]:
                    yield v
        elif self.op.op_enum == OperationEnum.lookup:
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

        if self.op.op_enum == OperationEnum.keys:
            yield from [(k, self[k]) for k in self.op.keys[skip : skip + limit]]
        elif self.op.op_enum == OperationEnum.distinct:
            query = self._build_mongo_query(self.op.select)
            # NOTE uid is guarenteed unique
            if self.op.distinct == "uid":
                for doc in (
                    self.metadata_collection.find(query, {"uid": 1})
                    .skip(skip)
                    .limit(limit)
                ):
                    k = doc["uid"]
                    yield (k, self[k])
            else:
                # FIXME wasteful to recompute this here (compute on construction?)
                for v in self.metadata_collection.find(query).distinct(
                    self.op.distinct
                )[skip : skip + limit]:
                    yield (v, self[v])
        elif self.op.op_enum == OperationEnum.lookup:
            raise RuntimeError("unreachable")
        else:
            raise RuntimeError("unreachable")

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)


def run_raw_mongo_query(query, tree):
    query = json.loads(query.query)
    return tree.new_variation(queries=tree.queries + [query])


AIMMCatalog.register_query(RawMongo, run_raw_mongo_query)
