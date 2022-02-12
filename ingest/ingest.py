import pathlib
from collections import defaultdict

import numpy as np
import pandas as pd
from tiled.examples.xdi import read_xdi

import models
import util


def ingest_wanli_oxygen_K(c, data_path, root=["core", "wanli"]):
    files = (data_path / "O_K").glob("*.txt")

    element = models.XDIElement(symbol="O", edge="K")

    util.mk_path(c, root)

    for f in files:
        name = f.stem
        print(f"{name}")
        df = pd.read_csv(f, header=None, delimiter="\t", names=["energy", "mu"])
        data = models.DataFrameData.from_pandas(df)
        xas_name = f"{element.symbol}-{element.edge}"
        xas = models.XASMeasurement(
            name=xas_name, element=element, metadata={}, data=data
        )
        path = "/" + "/".join(root + [name])
        sample = models.Sample(
            name=name, folder=False, path=path, metadata={}, measurements=[xas]
        )
        c.insert_one(sample.dict())


def ingest_wanli_TM_L(c, data_path, root=["core", "wanli"]):
    files = data_path / "TM_L"

    util.mk_path(c, root)

    for d in files.iterdir():
        if d.is_file():
            continue
        symbol = d.stem
        element = models.XDIElement(symbol=symbol, edge="L")

        for f in d.glob("*.txt"):
            if f.stem.startswith("IgorPlot"):
                continue

            name = f.stem
            print(f"{name}")

            with open(f, "r") as ff:
                l = ff.readline()
                n = len(l.split())

                if n == 1:
                    skiprows = 1
                    title = l
                elif n == 2:
                    skiprows = 0
                    title = None
                else:
                    assert False

            df = pd.read_csv(
                f, delimiter="\t", names=["energy", "mu"], skiprows=skiprows
            )

            data = models.DataFrameData.from_pandas(df)
            xas_name = f"{element.symbol}-{element.edge}"
            xas = models.XASMeasurement(
                name=xas_name, element=element, metadata={}, data=data
            )
            path = "/" + "/".join(root + [name])
            sample = models.Sample(
                name=name, folder=False, path=path, metadata={}, measurements=[xas]
            )
            c.insert_one(sample.dict())


def ingest_newville(c, data_path, root=["core", "newville"]):

    util.mk_path(c, root)

    files = list(data_path.rglob("*.xdi"))
    print(f"found {len(files)} xdi files to ingest")

    for f in files:
        name = f.stem
        print(f"{name}")

        df, metadata = read_xdi(str(f))
        fields = metadata.pop("fields")
        metadata.update(**fields)

        metadata = {k.lower(): v for k, v in metadata.items()}

        symbol = metadata["element"]["symbol"]
        edge = metadata["element"]["edge"]
        element = models.XDIElement(symbol=symbol, edge=edge)
        xas_name = f"{element.symbol}-{element.edge}"
        data = models.DataFrameData.from_pandas(df)
        xas = models.XASMeasurement(
            name=xas_name, element=element, metadata=metadata, data=data
        )
        path = "/" + "/".join(root + [name])
        sample = models.Sample(
            name=name, folder=False, path=path, metadata={}, measurements=[xas]
        )
        c.insert_one(sample.dict())
