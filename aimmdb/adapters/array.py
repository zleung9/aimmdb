import os
from sys import platform

import dask
import h5py
import numpy as np
from tiled.adapters.array import ArrayAdapter
from tiled.server.pydantic_array import ArrayStructure


def array_raise_if_inactive(method):
    def inner(self, *args, **kwargs):
        if self.array_adapter is None:
            raise ValueError("Not active")
        else:
            return method(self, *args, **kwargs)

    return inner


class WritingArrayAdapter:
    structure_family = "array"

    def __init__(self, metadata_collection, directory, doc):
        self.metadata_collection = metadata_collection
        self.directory = directory
        self.doc = doc
        self.array_adapter = None
        if self.doc.data_url is not None:
            path = self.doc.data_url.path
            if platform == "win32" and path[0] == "/":
                path = path[1:]

            file = h5py.File(path)
            dataset = file["data"]
            self.array_adapter = ArrayAdapter(dask.array.from_array(dataset))

    #        elif self.doc.data_blob is not None:
    #            self.array_adapter = ArrayAdapter(dask.array.from_array(self.doc.data_blob))

    @property
    def specs(self):
        return self.doc.specs

    @property
    def structure(self):
        return ArrayStructure.from_json(self.doc.structure)

    @property
    def metadata(self):
        out = self.doc.metadata.dict()
        _tiled = {"uid" : self.doc.uid}
        out["_tiled"] = _tiled
        return out

    @array_raise_if_inactive
    def read(self, *args, **kwargs):
        return self.array_adapter.read(*args, **kwargs)

    @array_raise_if_inactive
    def read_block(self, *args, **kwargs):
        return self.array_adapter.read_block(*args, **kwargs)

    def microstructure(self):
        return self.array_adapter.microstructure()

    def macrostructure(self):
        return self.array_adapter.macrostructure()

    def put_data(self, body):
        # Organize files into subdirectories with the first two
        # charcters of the uid to avoid one giant directory.
        path = self.directory / self.doc.uid[:2] / f"{self.doc.uid}.hdf5"
        path.parent.mkdir(parents=True, exist_ok=True)
        array = np.frombuffer(
            body, dtype=self.doc.structure.micro.to_numpy_dtype()
        ).reshape(self.doc.structure.macro.shape)
        with h5py.File(path, "w") as file:
            file.create_dataset("data", data=array)

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

    def delete(self):
        path = self.directory / self.doc.uid[:2] / f"{self.doc.uid}.hdf5"
        os.remove(path)
        result = self.metadata_collection.delete_one({"uid": self.doc.uid})
        assert result.deleted_count == 1
