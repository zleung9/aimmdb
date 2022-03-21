import pydantic
import fastapi
from fastapi import APIRouter, HTTPException, Request, Depends, Security

from tiled.server.authentication import get_current_principal
from tiled.server.dependencies import get_root_tree

from .models import PostDatasetData

router = APIRouter()


@router.post("/datasets")
def post_dataset(
    request: Request,
    body: PostDatasetData,
    principal=Security(get_current_principal),
    root=Depends(get_root_tree),
):
    print(f"{principal=}")
