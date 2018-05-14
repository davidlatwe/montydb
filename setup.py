import os
import imp
from setuptools import setup, find_packages


version_file = os.path.abspath("montydb/version.py")
version_mod = imp.load_source("version", version_file)
version = version_mod.version

setup(
    name="montydb",
    version=version,
    packages=find_packages(),

    # development metadata
    zip_safe=True,

    # metadata for upload to PyPI
    author="Lai, Ta Wei",
    author_email="david962041@gmail.com",
    description="",
    license="BSD 3-Clause",
    keywords="",
    url="https://github.com/davidlatwe/MontyDB",
)
