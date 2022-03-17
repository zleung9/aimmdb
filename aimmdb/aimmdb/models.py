import io
from enum import Enum
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pydantic
from bson.objectid import ObjectId
from pydantic import BaseModel, Extra, ValidationError, validator

from .serialization import serialize_parquet
from .utils import get_element_data


# see https://stackoverflow.com/questions/59503461/how-to-parse-objectid-in-a-pydantic-model
class PydanticObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, ObjectId):
            raise TypeError("ObjectId required")
        return v


# parquet is self-describing but we duplicate the metadata to make it
# accessible without reading the blob
class DataFrameData(BaseModel):
    columns: List[str]
    media_type: str
    blob: bytes

    @classmethod
    def from_pandas(cls, df):
        blob = serialize_parquet(df).tobytes()
        return cls(
            columns=list(df.columns), media_type="application/x-parquet", blob=blob
        )


# npy is self-describing but we duplicate the metadata to make it accessible
# without reading the blob
class ArrayData(BaseModel):
    shape: List[int]
    dtype: str
    media_type: str
    blob: bytes

    @classmethod
    def from_numpy(cls, x):
        with io.BytesIO() as f:
            np.save(f, x)
            blob = f.getvalue()
        return cls(
            shape=x.shape, dtype=x.dtype.str, media_type="application/x-npy", blob=blob
        )


class StructureFamilyEnum(str, Enum):
    node = "node"
    dataframe = "dataframe"
    array = "array"


class TiledData(BaseModel):
    structure_family: StructureFamilyEnum
    metadata: dict
    data: Union[None, ArrayData, DataFrameData]


class XDIElement(BaseModel):
    symbol: str
    edge: str

    @validator("symbol")
    def check_symbol(cls, s):
        symbols = get_element_data()["symbols"]
        if s not in symbols:
            raise ValueError(f"{s} not a valid element symbol")
        return s

    @validator("edge")
    def check_edge(cls, e):
        edges = get_element_data()["edges"]
        if e not in edges:
            raise ValueError(f"{e} not a valid edge")
        return e


class MeasurementEnum(str, Enum):
    xas = "xas"
    rixs = "rixs"


class ProvenanceData(BaseModel):
    source_id: str
    url: Optional[str]
    license: Optional[str]
    description: Optional[str]


class XASMetadata(BaseModel, extra=Extra.allow):
    name: str
    element: XDIElement
    measurement_type: MeasurementEnum = "xas"
    provenance: ProvenanceData


class XASMeasurement(TiledData):
    metadata: XASMetadata

    # TODO  does it make sense to have fields that extend TiledData like this?
    sample_id: PydanticObjectId


class SampleMetadata(BaseModel, extra=Extra.allow):
    provenance: ProvenanceData


class Sample(BaseModel):
    name: str
    metadata: SampleMetadata


class Node(BaseModel):
    name: str  # denormalized
    path: str
    metadata: dict  # denormalized
    structure_family: StructureFamilyEnum  # denormalized
    data_id: Union[None, PydanticObjectId]
