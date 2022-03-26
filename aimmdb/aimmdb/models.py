from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Extra, Field, validator

from .serialization import serialize_npy, serialize_parquet
from .utils import get_element_data


# parquet is self-describing but we duplicate the metadata to make it
# accessible without reading the blob
class DataFrameData(BaseModel):
    columns: List[str]
    media_type: str
    blob: bytes

    @classmethod
    def from_pandas(cls, df):
        return cls(
            columns=list(df.columns),
            media_type="application/x-parquet",
            blob=serialize_parquet(df).tobytes(),
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
        return cls(
            shape=x.shape,
            dtype=x.dtype.str,
            media_type="application/x-npy",
            blob=serialize_npy(x),
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
    source: str
    url: Optional[str]
    license: Optional[str]
    description: Optional[str]


class SampleData(BaseModel, extra=Extra.allow):
    uid: Optional[str] = Field(alias="_id")
    name: str
    dataset: str
    provenance: ProvenanceData


class XASMetadata(BaseModel, extra=Extra.allow):
    element: XDIElement
    measurement_type: MeasurementEnum = "xas"
    provenance: ProvenanceData
    sample_id: str


class XASMetadataDenormalized(BaseModel, extra=Extra.allow):
    element: XDIElement
    measurement_type: MeasurementEnum = "xas"
    provenance: ProvenanceData
    sample: SampleData


# FIXME clean this up with generic models???
class XASData(TiledData):
    uid: Optional[str] = Field(alias="_id")
    metadata: XASMetadata


class XASDataDenormalized(TiledData):
    uid: Optional[str] = Field(alias="_id")
    metadata: XASMetadataDenormalized
