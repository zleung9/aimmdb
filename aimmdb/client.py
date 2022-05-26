import operator
from typing import List, Optional

import msgpack
from tiled.client.dataframe import DataFrameClient
from tiled.client.node import Node
from tiled.client.utils import handle_error

import aimmdb
from aimmdb.schemas import XASMetadata, SampleData

def _describe_xas(*, element, edge, sample_name=None):
    desc = f"{element}-{edge}"
    if sample_name:
        desc = f"{sample_name} {desc}"
    return desc


class MongoCatalog(Node):
    pass


class SampleKey:
    def __init__(self, uid, name):
        self.uid = uid
        self.name = name

    def __repr__(self):
        return f"{self.name} ({self.uid})"

class XASKey:
    def __init__(self, uid, element, edge, sample_name=None):
        self.uid = uid
        self.element = element
        self.edge = edge
        self.sample_name = sample_name

    def __repr__(self):
        desc = _describe_xas(element=self.element, edge=self.edge, sample_name=self.sample_name)
        return f"{desc} ({self.uid})"

    @classmethod
    def from_client(cls, client):
        assert isinstance(client, XASClient)
        try:
            sample_name = client.metadata["sample"]["name"]
        except KeyError:
            sample_name = None
        return cls(uid=client.uid, element=client.element, edge=client.edge, sample_name=sample_name)


class AIMMCatalog(Node):
    def write_xas(self, df, metadata, specs=None):
        specs = list(specs or [])
        specs.append("XAS")

        validated_metadata = XASMetadata.parse_obj(metadata)
        key = self.write_dataframe(df, validated_metadata.dict(), specs=specs)

        return key

    def write_sample(self, metadata):
        sample = SampleData.parse_obj(metadata)
        document = self.context.post_json("/sample", sample.dict())
        uid = document["uid"]
        return uid

    def delete_sample(self, uid):
        self.context.delete_content(f"/sample/{uid}", None)

    def __getitem__(self, key):
        if isinstance(key, SampleKey):
            return super().__getitem__(key.uid)
        elif isinstance(key, XASKey):
            return super().__getitem__(key.uid)
        else:
            return super().__getitem__(key)

    def _keys_slice(self, start, stop, direction):
        op_dict = self.metadata["_tiled"]["op"]
        if (
            op_dict["op_enum"] == "distinct"
            and op_dict["distinct"] == "metadata.sample_id"
        ):
            for k, v in super()._items_slice(start, stop, direction):
                yield SampleKey(uid=k, name=v.metadata["_tiled"]["sample"]["name"])
        elif (
            op_dict["op_enum"] == "distinct"
            and op_dict["distinct"] == "uid"
            ):
            for k, v in super()._items_slice(start, stop, direction):
                if isinstance(v, XASClient):
                    k = XASKey.from_client(v)
                yield k
        else:
            yield from super()._keys_slice(start, stop, direction)

    def _items_slice(self, start, stop, direction):
        op_dict = self.metadata["_tiled"]["op"]
        if (
            op_dict["op_enum"] == "distinct"
            and op_dict["distinct"] == "metadata.sample_id"
        ):
            for k, v in super()._items_slice(start, stop, direction):
                yield (SampleKey(uid=k, name=v.metadata["_tiled"]["sample"]["name"]), v)
        elif (
            op_dict["op_enum"] == "distinct"
            and op_dict["distinct"] == "uid"
            ):
            for k, v in super()._items_slice(start, stop, direction):
                if isinstance(v, XASClient):
                    k = XASKey.from_client(v)
                yield k, v
        else:
            yield from super()._items_slice(start, stop, direction)


class XASClient(DataFrameClient):
    def describe(self):
        # sample name is optional
        try:
            sample_name = self.metadata["sample"]["name"]
        except KeyError:
            sample_name = None
        return _describe_xas(element=self.element, edge=self.edge, sample_name=sample_name)

    def __repr__(self):
        desc = self.describe()
        return f"<{type(self).__name__} ({desc})>"

    @property
    def uid(self):
        return self.metadata["_tiled"]["uid"]

    @property
    def element(self):
        return self.metadata["element"]["symbol"]

    @property
    def edge(self):
        return self.metadata["element"]["edge"]
