from typing import List, Tuple, Union

import pydantic


class DataFrameMicroStructure(pydantic.BaseModel):
    meta: bytes  # Arrow-encoded DataFrame
    divisions: bytes  # Arrow-encoded DataFrame


class DataFrameMacroStructure(pydantic.BaseModel):
    npartitions: int
    columns: List[str]
    resizable: Union[bool, Tuple[bool, ...]] = False


class DataFrameStructure(pydantic.BaseModel):
    micro: DataFrameMicroStructure
    macro: DataFrameMacroStructure
