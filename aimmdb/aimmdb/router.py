import msgpack
from fastapi import (APIRouter, Depends, Header, HTTPException, Request,
                     Security)
from tiled.server.authentication import get_current_principal
from tiled.server.dependencies import get_root_tree

from .models import SampleData, XASData, XASDataDenormalized
from .uid import uid


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
    sample.uid = uid()
    r = root.db.samples.insert_one(sample.dict(by_alias=True))
    return {"uid": str(r.inserted_id)}


@router.post(
    "/xas", dependencies=[Depends(has_write_permission), Depends(application_msgpack)]
)
async def post_xas(
    request: Request,
    root=Depends(get_root_tree),
):
    body = await request.body()
    xas = XASData.parse_obj(msgpack.unpackb(body))

    # denormalize sample on insertion
    sample = SampleData.parse_obj(
        root.db.samples.find_one({"_id": xas.metadata.sample_id})
    )

    xas_dict = xas.dict()
    xas_dict["metadata"].pop("sample_id")

    if "sample" in xas_dict["metadata"]:
        xas_dict["metadata"]["sample"].update(sample.dict(by_alias=True))
    else:
        xas_dict["metadata"]["sample"] = sample.dict(by_alias=True)

    xas_dict["_id"] = uid()
    xas_denormalized = XASDataDenormalized.parse_obj(xas_dict)

    r = root.db.measurements.insert_one(xas_denormalized.dict(by_alias=True))
    return {"uid": str(r.inserted_id)}
