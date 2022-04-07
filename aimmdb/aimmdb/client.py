from tiled.client.node import Node

from .uid import _uid_length

# node which appends some metadata to keys
class AnnotatedNodeBase(Node):
    def _annotate_key(self, key, value):
        raise NotImplementedError

    def __iter__(self):
        for k, v in self.items():
            yield k

    def __getitem__(self, key):
        if isinstance(key, str):
            key = key[:_uid_length]  # keys can have arbitrary suffixes
        return super().__getitem__(key)

    def items(self):
        for k, v in super().items():
            k = self._annotate_key(k, v)
            yield k, v

    def _keys_slice(self, start, stop, direction):
        for k, v in super()._items_slice(start, stop, direction):
            k = self._annotate_key(k, v)
            yield k

    def _items_slice(self, start, stop, direction):
        for k, v in super()._items_slice(start, stop, direction):
            k = self._annotate_key(k, v)
            yield k, v

    def _item_by_index(self, index, direction):
        k, v = super()._item_by_index(index, direction)
        k = self._annotate_key(k, v)
        return k, v


class CatalogOfMeasurements(AnnotatedNodeBase):
    # append some metadata to make keys more informative
    def _annotate_key(self, key, value):
        element = value.metadata["element"]["symbol"]
        edge = value.metadata["element"]["edge"]
        name = value.metadata["sample"]["name"]
        return f"{key} ({name} {element}-{edge})"


class CatalogOfSamples(AnnotatedNodeBase):
    def _annotate_key(self, key, value):
        name = value.metadata["sample"]["name"]
        return f"{key} ({name})"
