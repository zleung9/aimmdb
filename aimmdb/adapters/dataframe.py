import os
from sys import platform

import dask
import pandas as pd
from tiled.adapters.dataframe import DataFrameAdapter
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.structures.dataframe import deserialize_arrow

from aimmdb.schemas import Document


def dataframe_raise_if_inactive(method):
    def inner(self, *args, **kwargs):
        if self.dataframe_adapter is None:
            raise ValueError("Not active")
        else:
            return method(self, *args, **kwargs)

    return inner


class WritingDataFrameAdapter:
    structure_family = "dataframe"

    def __init__(self, metadata_collection, directory, doc):
        self.metadata_collection = metadata_collection
        self.directory = directory
        self.doc = Document(**doc)
        self.dataframe_adapter = None

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
    def structure(self):
        return DataFrameStructure.from_json(self.doc.structure)

    @property
    def metadata(self):
        return self.doc.metadata

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

    def put_data(self, body):
        # Organize files into subdirectories with the first two
        # charcters of the uid to avoid one giant directory.
        path = self.directory / self.doc.uid[:2] / self.doc.uid
        path.parent.mkdir(parents=True, exist_ok=True)

        dataframe = deserialize_arrow(body)

        dataframe.to_parquet(path)
        result = self.metadata_collection.update_one(
            {"_id": self.doc.uid},
            {
                "$set": {
                    "data_url": f"file://localhost/{str(path).replace(os.sep, '/')}"
                }
            },
        )

        assert result.matched_count == 1
        assert result.modified_count == 1
