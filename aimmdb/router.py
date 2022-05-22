from typing import Dict

import pydantic

from fastapi import APIRouter, Request, Security, HTTPException, Depends

from tiled.server.core import json_or_msgpack
from tiled.server.dependencies import get_root_tree, get_current_principal

from aimmdb.schemas import SampleData

class PostSampleResponse(pydantic.BaseModel):
    uid: str

router = APIRouter()

@router.post("/sample", response_model=PostSampleResponse)
def post_sample(
        request: Request,
        sample: SampleData,
        root=Security(get_root_tree, scopes=["write:data", "write:metadata"]),
        principal: str = Depends(get_current_principal),
):
    entry = root.authenticated_as(principal)
    try:
        uid = entry.post_sample(sample)
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="tree does not support posting sample metadata"
        )

    return json_or_msgpack(request, {"uid": uid})

@router.delete("/sample/{uid}")
def delete_sample(
        request: Request,
        uid: str,
        root=Security(get_root_tree, scopes=["write:data", "write:metadata"]),
        principal: str = Depends(get_current_principal),
):
    entry = root.authenticated_as(principal)
    try:
        entry.delete_sample(uid)
    except AttributeError:
        raise HTTPException(
            status_code=404, detail="tree does not support deleting sample metadata"
        )

    return json_or_msgpack(request, None)
