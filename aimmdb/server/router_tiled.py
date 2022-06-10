# place for routes to be upstreamed into tiled
import pydantic
from fastapi import APIRouter, HTTPException, Request, Security
from tiled.server.core import json_or_msgpack
from tiled.server.dependencies import entry

router = APIRouter()

# TODO /dataframe/partition
# TODO /array/block
# TODO xarray
