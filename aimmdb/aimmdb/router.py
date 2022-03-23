import pydantic
import fastapi
import msgpack
from fastapi import APIRouter, HTTPException, Request, Depends, Security, Header

from tiled.server.authentication import get_current_principal
from tiled.server.dependencies import get_root_tree

from .models import SampleData, XASData


def application_msgpack(content_type: str = Header(...)):
    """Require request MIME-type to be application/msgpack"""

    if content_type != "application/msgpack":
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported media type: {content_type}."
            " It must be application/msgpack",
        )


def has_write_permission(
    principal=Security(get_current_principal, scopes=["write:data", "write:metadata"]),
    root=Depends(get_root_tree),
):
    if not root.access_policy.has_write_permission(principal):
        raise HTTPException(
            status_code=403, detail="principal does not have write permission"
        )


router = APIRouter()


@router.post("/samples", dependencies=[Depends(has_write_permission)])
def post_sample(
    request: Request,
    sample: SampleData,
    root=Depends(get_root_tree),
):
    try:
        r = root.db.samples.insert_one(sample.dict())
        return {"uid": str(r.inserted_id)}
    except Exception as e:
        print(f"post_sample: {e}")  # FIXME properly log this
        raise HTTPException(status_code=422, detail=f"unable to insert sample")


@router.post(
    "/xas", dependencies=[Depends(has_write_permission), Depends(application_msgpack)]
)
async def post_xas(
    request: Request,
    root=Depends(get_root_tree),
):
    try:
        body = await request.body()
        xas = XASData.parse_obj(msgpack.unpackb(body))
        r = root.db.measurements.insert_one(xas.dict())
        return {"uid": str(r.inserted_id)}
    except Exception as e:
        print(f"post_xas: {e}")  # FIXME properly log this
        raise HTTPException(status_code=422, detail=f"unable to insert xas measurement")
