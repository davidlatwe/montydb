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
    packages=find_packages(exclude=("tests",)),

    # development metadata
    zip_safe=True,

    # metadata for upload to PyPI
    author="davidlatwe",
    author_email="david962041@gmail.com",
    description="A serverless Mongo-like database backed with SQLite.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="BSD-3-Clause",
    keywords="database nosql mongodb",
    url="https://github.com/davidlatwe/MontyDB",
    classifiers=(
        "Development Status :: 1 - Planning",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Operating System :: OS Independent",
    ),
    install_requires=(
        "pyyaml",
        "jsonschema",
        "pymongo",
    ),
    tests_require=(
        "pytest-cov",
    ),
)
