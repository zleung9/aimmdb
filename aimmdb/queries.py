import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

import pydantic
from tiled.queries import Comparison, Contains, Eq, register
from tiled.query_registration import register

JSONSerializable = Any  # Feel free to refine this.


@register(name="in")
@dataclass
class In:
    """
    Query if a given key's value is present in the specified list of values.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for color in ["red", "blue"]

    >>> c.search(In("color", ["red", "blue"]))
    """

    key: str
    value: List[JSONSerializable]

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


@register(name="notin")
@dataclass
class NotIn:
    """
    Query if a given key's value is not present in the specified list of values.

    Parameters
    ----------
    key : str
        e.g. "color", "sample.name"
    value : JSONSerializable
        May be a string, number, list, or dict.

    Examples
    --------

    Search for color in ["red", "blue"]

    >>> c.search(In("color", ["red", "blue"]))
    """

    key: str
    value: List[JSONSerializable]

    def encode(self):
        return {"key": self.key, "value": json.dumps(self.value)}

    @classmethod
    def decode(cls, *, key, value):
        return cls(key=key, value=json.loads(value))


def make_mongo_query_in(query, prefix=None):
    assert isinstance(query, In)
    mongo_key = ".".join([prefix, query.key]) if prefix else query.key
    mongo_query = {mongo_key: {"$in": query.value}}
    return mongo_query


def make_mongo_query_notin(query, prefix=None):
    assert isinstance(query, NotIn)
    mongo_key = ".".join([prefix, query.key]) if prefix else query.key
    mongo_query = {mongo_key: {"$nin": query.value}}
    return mongo_query


def make_mongo_query_eq(query, prefix=None):
    assert isinstance(query, Eq)
    mongo_key = ".".join([prefix, query.key]) if prefix else query.key
    mongo_query = {mongo_key: {"$eq": query.value}}
    return mongo_query


def make_mongo_query_comparison(query, prefix=None):
    assert isinstance(query, Comparison)
    op = query.operator
    if op not in {"le", "lt", "ge", "gt"}:
        raise ValueError(f"Unexpected operator {query.operator}.")
    mongo_op = {"lt": "$lt", "le": "$lte", "gt": "$gt", "ge": "$gte"}[op]
    mongo_key = ".".join([prefix, query.key]) if prefix else query.key
    mongo_query = {mongo_key: {mongo_op: query.value}}
    return mongo_query


def make_mongo_query_contains(query, prefix=None):
    assert isinstance(query, Contains)
    mongo_key = ".".join([prefix, query.key]) if prefix else query.key
    mongo_query = {mongo_key: query.value}
    return mongo_query


def run_eq(query, tree):
    mongo_query = make_mongo_query_eq(query, prefix="metadata")
    return tree.new_variation(queries=tree.queries + [mongo_query])


def run_comparison(query, tree):
    mongo_query = make_mongo_query_comparison(query, prefix="metadata")
    return tree.new_variation(queries=tree.queries + [mongo_query])


def run_contains(query, tree):
    mongo_query = make_mongo_query_contains(query, prefix="metadata")
    return tree.new_variation(queries=tree.queries + [mongo_query])


def run_in(query, tree):
    mongo_query = make_mongo_query_in(query, prefix="metadata")
    return tree.new_variation(queries=tree.queries + [mongo_query])


def run_notin(query, tree):
    mongo_query = make_mongo_query_notin(query, prefix="metadata")
    return tree.new_variation(queries=tree.queries + [mongo_query])


def register_queries_helper(cls):
    cls.register_query(Eq, run_eq)
    cls.register_query(Comparison, run_comparison)
    cls.register_query(Contains, run_contains)
    cls.register_query(In, run_in)
    cls.register_query(NotIn, run_notin)


class OperationEnum(str, Enum):
    distinct = "distinct"
    lookup = "lookup"
    keys = "keys"


class Distinct(pydantic.BaseModel):
    op_enum: OperationEnum = "distinct"
    select: Dict
    distinct: str


class Keys(pydantic.BaseModel):
    op_enum: OperationEnum = "keys"
    select: Dict
    keys: List[str]


class Lookup(pydantic.BaseModel):
    op_enum: OperationEnum = "lookup"
    select: Dict

    @pydantic.validator("select")
    def check_select(cls, select):
        if "uid" not in select:
            raise ValueError("Lookup operation must have uid specified")
        return select


def parse_path(path, key_to_query):
    valid_keys = set(key_to_query.keys())
    keys = path[0::2]
    values = path[1::2]

    if not set(keys).issubset(valid_keys):
        invalid_keys = set(keys) - valid_keys
        raise KeyError(f"keys {invalid_keys} not in {valid_keys}")

    select = {key_to_query[k]: v for k, v in zip(keys, values)}
    leftover_keys = valid_keys - set(keys)
    # if we have more keys then values then get distinct values for the last key
    if len(keys) == len(values) + 1:
        return Distinct(select=select, distinct=key_to_query[keys[-1]])

    # if keys and values are matched then perform a lookup if uid was provided otherwise get remaining keys
    elif len(keys) == len(values):
        if "uid" in keys:
            return Lookup(select=select)
        else:
            return Keys(select=select, keys=leftover_keys)

    raise KeyError(f"{len(keys)=}, {len(values)=}")
