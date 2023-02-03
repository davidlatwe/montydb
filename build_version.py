"""

"""
import os
import sys


def write_version(ver_str):
    root = os.path.dirname(__file__)
    files = [
        os.path.join(root, "montydb", "_version.py"),
        os.path.join(root, "pyproject.toml")
    ]

    for file in files:
        with open(file, "r") as f:
            content = f.read()
        with open(file, "w") as f:
            f.write(content.replace("99.dev.0", ver_str))


if __name__ == "__main__":
    write_version(sys.argv[1])
