# NOTE these routes are all meant to be upstreamed into tiled

import base64
from typing import Dict, List

import pydantic
from fastapi import APIRouter, Request, Security, HTTPException
from tiled.server.core import json_or_msgpack
from tiled.server.dependencies import entry
from tiled.server.schemas import Structure
from tiled.structures.core import StructureFamily
from tiled.structures.dataframe import deserialize_arrow


class PostMetadataRequest(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Structure
    metadata: Dict
    specs: List[str]


class PostMetadataResponse(pydantic.BaseModel):
    uid: str


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

    try:
        uid = entry.post_metadata(
            metadata=body.metadata,
            structure_family=body.structure_family,
            structure=body.structure,
            specs=body.specs,
        )
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="entry does not support posting metadata"
        )

    return json_or_msgpack(request, {"uid": uid})


@router.put("/array/full/{path:path}")
async def put_array_full(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    data = await request.body()
    try:
        entry.put_data(data)
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="entry does not support putting data"
        )

    return json_or_msgpack(request, None)


@router.put("/node/full/{path:path}")
async def put_dataframe_full(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    data = await request.body()
    try:
        entry.put_data(data)
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="entry does not support putting data"
        )
    return json_or_msgpack(request, None)


@router.delete("/node/delete/{path:path}")
async def delete(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    try:
        entry.delete()
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="entry does not support deletion"
        )
    return json_or_msgpack(request, None)


# TODO /dataframe/partition
# TODO /array/block
# TODO xarray
