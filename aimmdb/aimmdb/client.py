import operator

from tiled.client.node import Node
from tiled.client.dataframe import DataFrameClient

from .uid import _uid_length

class AIMMCatalog(Node):
    def __repr__(self):
        element = self.metadata["element"].get("symbol", "*")
        edge = self.metadata["element"].get("edge", "*")

        sample_id = self.metadata["sample"].get("_id", None)
        sample_name = self.metadata["sample"].get("name", None)

        sample_repr = ""
        if sample_name:
            sample_repr = f"{sample_name} ({sample_id}) "

        out = f"<{type(self).__name__} ({sample_repr}{element}-{edge}) {{"

        N = 10
        keys = self._keys_slice(0, N, direction=1)
        key_reprs = list(map(repr, keys))

        if key_reprs:
            out += key_reprs[0]

        counter = 1
        for key_repr in key_reprs[1:]:
            if len(out) + len(key_repr) > 80:
                break
            out += ", " + key_repr
            counter += 1

        approx_len = operator.length_hint(self)  # cheaper to compute than len(tree)
        # Are there more in the tree that what we displayed above?
        if approx_len > counter:
            out += f", ...}} ~{approx_len} entries>"
        else:
            out += "}>"
        return out

class XASClient(DataFrameClient):
    def __repr__(self):
        element = self.metadata["element"]["symbol"]
        edge = self.metadata["element"]["edge"]
        name = self.metadata["sample"]["name"]
        return f"<{type(self).__name__} ({name} {element}-{edge})>"
