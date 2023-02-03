import os
import toml
from pathlib import Path
import montydb


def test_versions_are_in_sync():
    """Checks if the pyproject.toml and package.__init__.py __version__ are in sync."""

    path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    pyproject = toml.loads(open(str(path)).read())
    pyproject_version = pyproject["tool"]["poetry"]["version"]

    package_version = montydb.__version__
    assert package_version == pyproject_version

    if os.getenv("GITHUB_REF_TYPE") == "tag":
        # On package releasing
        assert package_version == os.getenv("GITHUB_REF_NAME")
        assert all(v.isdigit() for v in package_version.split("."))
