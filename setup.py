import os
import imp
from setuptools import setup, find_packages


version_file = os.path.abspath("montydb/version.py")
version_mod = imp.load_source("version", version_file)
version = version_mod.version

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="montydb",
    version=version,
    packages=find_packages(exclude=("tests", "tests.*")),

    # development metadata
    zip_safe=True,

    # metadata for upload to PyPI
    author="davidlatwe",
    author_email="davidlatwe@gmail.com",
    description="MongoDB's unofficial Python implementation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=["monty", "montydb", "mongo", "mongodb", "pymongo"],
    url="https://github.com/davidlatwe/montydb",
    license="BSD-3-Clause",
    python_requires=">=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*",
    classifiers=(
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
    ),
    install_requires=(
        "pymongo",
    ),
    tests_require=(
        "pytest-cov",
    ),
)
