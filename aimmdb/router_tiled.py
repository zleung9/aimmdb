# NOTE these routes are all meant to be upstreamed into tiled

import base64
from typing import Dict, List, Union

import pydantic
from fastapi import APIRouter, Request, Security
from tiled.server.core import json_or_msgpack
from tiled.server.dependencies import entry
from tiled.server.pydantic_array import ArrayStructure
from tiled.server.pydantic_dataframe import DataFrameStructure
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import deserialize_arrow
from tiled.server.schemas import Structure


class PostMetadataRequest(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Structure
    metadata: Dict
    specs: List[str]


class PostMetadataResponse(pydantic.BaseModel):
    loc: str


router = APIRouter()


@router.post("/node/metadata/{path:path}", response_model=PostMetadataResponse)
def post_metadata(
    request: Request,
    body: PostMetadataRequest,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    if body.structure_family == StructureFamily.dataframe:
        # Decode meta
        meta = body.structure.micro["meta"]
        body.structure.micro["meta"] = deserialize_arrow(base64.b64decode(meta))

    uid = entry.post_metadata(
        metadata=body.metadata,
        structure_family=body.structure_family,
        structure=body.structure,
        specs=body.specs,
    )
    return json_or_msgpack(request, {"uid": uid})


@router.put("/array/full/{path:path}")
async def put_array_full(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    data = await request.body()
    entry.put_data(data)
    return json_or_msgpack(request, None)


# FIXME should this become /node/full?
@router.put("/dataframe/full/{path:path}")
async def put_dataframe_full(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    data = await request.body()
    entry.put_data(data)
    return json_or_msgpack(request, None)
