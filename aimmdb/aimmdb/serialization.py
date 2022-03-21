import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def serialize_parquet(df):
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    buf = sink.getvalue()
    return memoryview(sink.getvalue())


def deserialize_parquet(data):
    reader = pa.BufferReader(data)
    table = pq.read_table(reader)
    return table.to_pandas()


def serialize_hdf5(node, metadata):
    buffer = io.BytesIO()
    with h5py.File(buffer, mode="w") as file:
        for (x, pre) in walk(node):
            path = "/".join(pre)
            if x is not None:
                file[path] = x
            else:
                file[path] = h5py.Empty("f")

    return buffer.getbuffer()
