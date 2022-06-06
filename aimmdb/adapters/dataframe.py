import os
from sys import platform

import dask
import pandas as pd
from fastapi import HTTPException
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.structures.dataframe import DataFrameStructure, deserialize_arrow

from aimmdb.access import require_write_permission


def dataframe_raise_if_inactive(method):
    def inner(self, *args, **kwargs):
        if self.dataframe_adapter is None:
            raise ValueError("Not active")
        else:
            return method(self, *args, **kwargs)

    return inner


# FIXME write specs
class WritingDataFrameAdapter:
    structure_family = "dataframe"

    def __init__(self, metadata_collection, directory, doc, permissions=None):
        self.metadata_collection = metadata_collection
        self.directory = directory
        self.doc = doc
        self.dataframe_adapter = None
        self.permissions = list(permissions or [])

        if self.doc.data_url is not None:
            path = self.doc.data_url.path
            if platform == "win32" and path[0] == "/":
                path = path[1:]

            self.dataframe_adapter = DataFrameAdapter(
                dask.dataframe.from_pandas(
                    pd.read_parquet(path),
                    npartitions=self.doc.structure.macro.npartitions,
                )
            )

    #        elif self.doc.data_blob is not None:
    #            self.dataframe_adapter = DataFrameAdapter(
    #                dask.dataframe.from_pandas(
    #                    deserialize_arrow(base64.b64decode(self.doc.data_blob)),
    #                    npartitions=self.doc.structure.macro.npartitions,
    #                )
    #            )

    @property
    def specs(self):
        return self.doc.specs

    @property
    def structure(self):
        return DataFrameStructure.from_json(self.doc.structure)

    @property
    def metadata(self):
        out = self.doc.metadata.dict()
        _tiled = {"uid": self.doc.uid}
        out["_tiled"] = _tiled
        return out

    @dataframe_raise_if_inactive
    def read(self, *args, **kwargs):
        return self.dataframe_adapter.read(*args, **kwargs)

    @dataframe_raise_if_inactive
    def read_partition(self, *args, **kwargs):
        return self.dataframe_adapter.read_partition(*args, **kwargs)

    def microstructure(self):
        return self.dataframe_adapter.microstructure()

    def macrostructure(self):
        return self.dataframe_adapter.macrostructure()

    @require_write_permission
    def put_data(self, body):

        # Organize files into subdirectories with the first two
        # charcters of the uid to avoid one giant directory.
        path = self.directory / self.doc.uid[:2] / self.doc.uid
        path.parent.mkdir(parents=True, exist_ok=True)

        dataframe = deserialize_arrow(body)

        dataframe.to_parquet(path)
        result = self.metadata_collection.update_one(
            {"uid": self.doc.uid},
            {
                "$set": {
                    "data_url": f"file://localhost/{str(path).replace(os.sep, '/')}"
                }
            },
        )

        assert result.matched_count == 1
        assert result.modified_count == 1

    @require_write_permission
    def delete(self):
        path = self.directory / self.doc.uid[:2] / self.doc.uid
        # FIXME handle case where file does not exist
        os.remove(path)
        result = self.metadata_collection.delete_one({"uid": self.doc.uid})
        assert result.deleted_count == 1
