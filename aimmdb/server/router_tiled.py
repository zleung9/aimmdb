# NOTE these routes are all meant to be upstreamed into tiled
import pydantic
from fastapi import APIRouter, HTTPException, Request, Security
from tiled.server.core import json_or_msgpack
from tiled.server.dependencies import entry

router = APIRouter()

@router.delete("/node/delete/{path:path}")
async def delete(
    request: Request,
    entry=Security(entry, scopes=["write:data", "write:metadata"]),
):
    if hasattr(entry, "delete"):
        entry.delete()
    else:
        raise HTTPException(status_code=404, detail="entry does not support deletion")
    return json_or_msgpack(request, None)


# TODO /dataframe/partition
# TODO /array/block
# TODO xarray
