from enum import Enum
from typing import Dict, List, Optional, Union

import pydantic
from bson.objectid import ObjectId
from pydantic import BaseModel

import io
import pandas as pd
import numpy as np

from serialization import serialize_parquet


class MeasurementEnum(str, Enum):
    xas = "xas"
    rixs = "rixs"


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


class XDIElement(BaseModel):
    symbol: str
    edge: str


class MeasurementBase(BaseModel):
    name: str


class XASMeasurement(MeasurementBase):
    element: XDIElement
    metadata: dict
    data: DataFrameData
    type: MeasurementEnum = "xas"


class RIXSMeasurement(MeasurementBase):
    element: XDIElement
    metadata: dict
    data: ArrayData
    type: MeasurementEnum = "rixs"


class TreeNode(BaseModel):
    name: str
    folder: bool = False
    path: str


class Sample(TreeNode):
    metadata: dict
    measurements: List[MeasurementBase]


class Folder(TreeNode):
    metadata: dict
