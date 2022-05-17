import json
from dataclasses import dataclass
from enum import Enum

from tiled.queries import register


@register(name="raw_mongo")
@dataclass
class RawMongo:
    """
    Run a MongoDB query against a given collection.
    """

    query: str  # We cannot put a dict in a URL, so this a JSON str.

    def __init__(self, query):
        if not isinstance(query, str):
            query = json.dumps(query)
        self.query = query


class OperationEnum(str, Enum):
    distinct = "distinct"
    lookup = "lookup"
    keys = "keys"


def parse_path(path, key_translation):
    valid_keys = set(key_translation.keys())
    keys = path[0::2]
    values = path[1::2]

    if not set(keys).issubset(valid_keys):
        invalid_keys = set(keys) - valid_keys
        raise KeyError(f"keys {invalid_keys} not in {valid_keys}")

    select = {key_translation[k]: v for k, v in zip(keys, values)}
    leftover_keys = valid_keys - set(keys)

    # if we have more keys then values then get distinct values for the last key
    if len(keys) == len(values) + 1:
        operation = (
            OperationEnum("distinct"),
            {"select": select, "distinct": key_translation[keys[-1]]},
        )
    # if keys and values are matched then perform a lookup if uid was provided otherwise get remaining keys
    elif len(keys) == len(values):
        if "uid" in keys:
            operation = (OperationEnum("lookup"), {"select": select})
        else:
            operation = (
                OperationEnum("keys"),
                {"keys": leftover_keys, "select": select},
            )
    else:
        raise KeyError(f"{len(keys)=}, {len(values)=}")

    return operation
