from enum import Enum
from typing import Dict, Generic, List, Optional, TypeVar, Union

import pydantic
import pydantic.generics
from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.structures.core import StructureFamily
from tiled.structures.xarray import DataArrayStructure, DatasetStructure

from aimmdb.utils import get_element_data

structure_association = {
    StructureFamily.array: ArrayStructure,
    StructureFamily.dataframe: DataFrameStructure,
    StructureFamily.xarray_data_array: DataArrayStructure,
    StructureFamily.xarray_dataset: DatasetStructure,
}

MetadataT = TypeVar("MetadataT")


class GenericDocument(pydantic.generics.GenericModel, Generic[MetadataT]):
    uid: Optional[str]
    structure_family: StructureFamily
    structure: Union[ArrayStructure, DataFrameStructure]
    metadata: MetadataT
    specs: List[str]
    mimetype: str
    data_blob: Optional[bytes]
    data_url: Optional[pydantic.AnyUrl]

    @pydantic.root_validator(skip_on_failure=True)
    def validate_structure_matches_structure_family(cls, values):
        # actual_structure_type = cls.__annotations__["structure"]  # this is what was filled in for StructureT
        actual_structure = values.get("structure")
        # Given the structure_family, we know what the structure type should be.
        expected_structure_type = structure_association[values.get("structure_family")]
        if values.get("expected_structure_type") == StructureFamily.node:
            raise Exception(
                f"{expected_structure_type} is not currently supported as a writable structure"
            )
        elif not isinstance(actual_structure, expected_structure_type):
            raise Exception(
                "The expected structure type does not match the received structure type"
            )
        return values

    @pydantic.root_validator(skip_on_failure=True)
    def check_data_source(cls, values):
        # Making them optional and setting default values might help to meet these conditions
        # with the current data types without getting any conflicts
        # if values.get('data_blob') is None and values.get('data_url') is None:
        #     raise ValueError("Not Valid: data_blob and data_url are both None. Use one of them")
        if values.get("data_blob") is not None and values.get("data_url") is not None:
            raise ValueError(
                "Not Valid: data_blob and data_url contain values. Use just one"
            )
        return values

    @pydantic.validator("mimetype")
    def is_mime_type(cls, v):
        m_type, _, _ = v.partition("/")
        mime_type_list = set(
            [
                "application",
                "audio",
                "font",
                "example",
                "image",
                "message",
                "model",
                "multipart",
                "text",
                "video",
            ]
        )

        if m_type not in mime_type_list:
            raise ValueError(f"{m_type} is not a valid mime type")
        return v


class XDIElement(pydantic.BaseModel):
    symbol: str
    edge: str

    @pydantic.validator("symbol")
    def check_symbol(cls, s):
        symbols = get_element_data()["symbols"]
        if s not in symbols:
            raise ValueError(f"{s} not a valid element symbol")
        return s

    @pydantic.validator("edge")
    def check_edge(cls, e):
        edges = get_element_data()["edges"]
        if e not in edges:
            raise ValueError(f"{e} not a valid edge")
        return e


class MeasurementEnum(str, Enum):
    xas = "xas"
    rixs = "rixs"


class XASMetadata(pydantic.BaseModel, extra=pydantic.Extra.allow):
    element: XDIElement
    measurement_type: MeasurementEnum = "xas"
    dataset: str
    sample_id: Optional[str]

Document = GenericDocument[Dict]

class XASDocument(GenericDocument[XASMetadata]):
    @pydantic.validator("specs")
    def check_specs(cls, specs):
        if "XAS" not in specs:
            raise ValueError(f"{specs=}")
        return specs

    @pydantic.validator("structure_family")
    def check_structure_family(cls, structure_family):
        if structure_family != StructureFamily.dataframe:
            raise ValueError(f"{structure_family=}")
        return structure_family

class MetadataWithDataset(pydantic.BaseModel, extra=pydantic.Extra.allow):
    dataset: str

DocumentWithDataset = GenericDocument[MetadataWithDataset]

# from enum import Enum
# from typing import List, Optional, Union
#
# from pydantic import BaseModel, Extra, Field, validator
#
# from .serialization import serialize_npy, serialize_parquet
# from .utils import get_element_data
#
#
# class ProvenanceData(BaseModel):
#    source: str
#    url: Optional[str]
#    license: Optional[str]
#    description: Optional[str]
#
#
# class SampleData(BaseModel, extra=Extra.allow):
#    uid: Optional[str] = Field(alias="_id")
#    name: str
#    provenance: ProvenanceData
#
#
# class XASMetadata(BaseModel, extra=Extra.allow):
#    element: XDIElement
#    measurement_type: MeasurementEnum = "xas"
#    provenance: ProvenanceData
#    dataset: str  # FIXME is dataset just a tag or does it deserve to be separate
#    sample_id: str
#
#
# class XASMetadataDenormalized(BaseModel, extra=Extra.allow):
#    element: XDIElement
#    measurement_type: MeasurementEnum = "xas"
#    provenance: ProvenanceData
#    dataset: str
#    sample: SampleData
#
#
## FIXME clean this up with generic models???
# class XASData(TiledData):
#    uid: Optional[str] = Field(alias="_id")
#    metadata: XASMetadata
#
#
# class XASDataDenormalized(TiledData):
#    uid: Optional[str] = Field(alias="_id")
#    metadata: XASMetadataDenormalized
