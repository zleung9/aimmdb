import operator
from typing import List, Optional

import msgpack
from tiled.client.dataframe import DataFrameClient
from tiled.client.node import Node
from tiled.client.utils import handle_error

import aimmdb
from aimmdb.schemas import XASMetadata


class MongoCatalog(Node):
    def __delitem__(self, key):
        path = (
            "/node/delete/"
            + "".join(
                f"/{part}" for part in self.context.path_parts
            )  # FIXME this should be a prefix
            + "".join(f"/{part}" for part in self._path)
            + "/"
            + key
        )

        # Submit CSRF token in both header and cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        headers = {}
        headers.setdefault("x-csrf", self.context._client.cookies["tiled_csrf"])
        headers.setdefault("accept", "application/x-msgpack")
        request = self.context._client.build_request(
            "DELETE",
            path,
            content=None,
            headers=headers,
        )
        response = self.context._client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )


class AIMMCatalog(Node):
    def __delitem__(self, key):
        path = (
            "/node/delete/"
            + "".join(
                f"/{part}" for part in self.context.path_parts
            )  # FIXME this should be a prefix
            + "".join(f"/{part}" for part in self._path)
            + "/"
            + key
        )

        # Submit CSRF token in both header and cookie.
        # https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#double-submit-cookie
        headers = {}
        headers.setdefault("x-csrf", self.context._client.cookies["tiled_csrf"])
        headers.setdefault("accept", "application/x-msgpack")
        request = self.context._client.build_request(
            "DELETE",
            path,
            content=None,
            headers=headers,
        )
        response = self.context._client.send(request)
        handle_error(response)
        return msgpack.unpackb(
            response.content,
            timestamp=3,  # Decode msgpack Timestamp as datetime.datetime object.
        )

    def write_xas(self, df, metadata, specs=None):
        specs = list(specs or [])
        specs.append("XAS")

        validated_metadata = XASMetadata.parse_obj(metadata)
        self.write_dataframe(df, validated_metadata.dict(), specs=specs)


class XASClient(DataFrameClient):
    def __repr__(self):
        # this metadata are required
        element = self.metadata["element"]["symbol"]
        edge = self.metadata["element"]["edge"]
        desc = f"{element}-{edge}"

        # sample name is optional
        try:
            name = self.metadata["sample"]["name"]
        except KeyError:
            name = None

        if name:
            desc = f"{name} {desc}"

        return f"<{type(self).__name__} ({desc})>"


# class AIMMCatalog(Node):
#    def __repr__(self):
#        element = self.metadata["element"].get("symbol", "*")
#        edge = self.metadata["element"].get("edge", "*")
#
#        sample_id = self.metadata["sample"].get("_id", None)
#        sample_name = self.metadata["sample"].get("name", None)
#
#        sample_repr = ""
#        if sample_name:
#            sample_repr = f"{sample_name} ({sample_id}) "
#
#        out = f"<{type(self).__name__} ({sample_repr}{element}-{edge}) {{"
#
#        N = 10
#        keys = self._keys_slice(0, N, direction=1)
#        key_reprs = list(map(repr, keys))
#
#        if key_reprs:
#            out += key_reprs[0]
#
#        counter = 1
#        for key_repr in key_reprs[1:]:
#            if len(out) + len(key_repr) > 80:
#                break
#            out += ", " + key_repr
#            counter += 1
#
#        approx_len = operator.length_hint(self)  # cheaper to compute than len(tree)
#        # Are there more in the tree that what we displayed above?
#        if approx_len > counter:
#            out += f", ...}} ~{approx_len} entries>"
#        else:
#            out += "}>"
#        return out
#
#    def post_sample(self, metadata):
#        sample = SampleData.parse_obj(metadata)
#        request = self.context._client.build_request(
#            "POST", "/samples", json=sample.dict()
#        )
#
#        r = self.context._send(request)
#        if not r.status_code == 200:
#            print(r.json())
#            assert False
#
#        data = r.json()
#        if "uid" in data:
#            sample_id = data["uid"]
#        else:
#            raise RuntimeError(data)
#
#        return sample_id
#
#    def delete_sample(self, uid):
#        request = self.context._client.build_request("DELETE", f"/samples/{uid}")
#        r = self.context._send(request)
#        if not r.status_code == 200:
#            assert False
#
#    def post_xas(self, df, metadata):
#        data = aimmdb.models.DataFrameData.from_pandas(df)
#
#        measurement = aimmdb.models.XASData(
#            structure_family="dataframe",
#            metadata=metadata,
#            data=data,
#        )
#
#        request = self.context._client.build_request(
#            "POST",
#            "/xas",
#            content=msgpack.packb(measurement.dict()),
#            headers={"content-type": "application/msgpack"},
#        )
#
#        r = self.context._send(request)
#        if not r.status_code == 200:
#            assert False
#
#    def delete_xas(self, uid):
#        request = self.context._client.build_request("DELETE", f"/xas/{uid}")
#        r = self.context._send(request)
#        if not r.status_code == 200:
#            assert False
#
#
