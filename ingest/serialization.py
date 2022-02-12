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
