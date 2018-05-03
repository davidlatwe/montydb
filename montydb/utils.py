from bson.json_util import (
    loads as _loads,
    dumps as _dumps,
    RELAXED_JSON_OPTIONS as _default_json_opts,
)


__all__ = [
    "monty_load",
    "monty_dump",
]


def monty_load(file_path, json_options=None):
    opt = json_options if json_options else _default_json_opts
    with open(file_path, "r") as fp:
        lines = []
        for line in (ln.strip() for ln in fp.readlines()):
            lines += [line] if not line.startswith("//") else []
        serialized = "[{}]".format(", ".join(lines))
    return _loads(serialized, json_options=opt)


def monty_dump(file_path, documents, json_options=None):
    if not isinstance(documents, list):
        raise TypeError("Param `documents` should be a list.")

    opt = json_options if json_options else _default_json_opts
    serialized = [_dumps(doc, json_options=opt) for doc in documents]
    with open(file_path, "w") as fp:
        fp.write("\n".join(serialized))
