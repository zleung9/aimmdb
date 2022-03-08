from setuptools import setup, find_packages

setup(
    name="aimmdb",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "tiled",
        "pymongo",
        "pyarrow",
        "ariadne",
        ],
    python_requires="~=3.8",
    entry_points={},
)
