import io

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def serialize_npy(x):
    with io.BytesIO() as f:
        np.save(f, x)
        buf = f.getvalue()
        return memoryview(buf)


def serialize_parquet(df):
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    buf = sink.getvalue()
    return memoryview(buf)


def deserialize_parquet(data):
    reader = pa.BufferReader(data)
    table = pq.read_table(reader)
    return table.to_pandas()
