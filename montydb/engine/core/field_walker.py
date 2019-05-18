

class _NoVal(object):
    def __repr__(self):
        return "_NoVal()"
    __solts__ = ()


_no_val = _NoVal()


class FieldWalkError(Exception):
    """Base class for FieldWalker exceptions."""


class FieldWriteError(FieldWalkError):
    """FieldWalker write operation error class"""


class FieldCreateError(FieldWriteError):
    """Raised when creating field on immutable value."""


class FieldConflictError(FieldWriteError):
    """Raised when field path overlapping between write operations."""


class PositionalWriteError(FieldWriteError):
    """Raised when field path overlapping between write operations."""


class FieldValues(object):
    __slots__ = ("nodes", "values", "exists", "null_or_missing",
                 "matched_node", "_value_iter", "__iter")

    def __init__(self, nodes):
        self.nodes = nodes
        self.values = list(self._iter(False, True, False))
        self.exists = any(nd.exists for nd in nodes)
        self.null_or_missing = (any(nd.is_missing() for nd in nodes) or
                                self.exists and None in self.values)

        self.matched_node = None
        self._value_iter = self.iter_full

    def _iter(self, array_only, unpack, pack):
        for node in self.nodes:
            self.matched_node = node

            doc = node.value
            if isinstance(doc, list):
                # docs in array
                if unpack and not node.located:
                    for i, elem in enumerate(doc):
                        if elem is not _no_val:
                            self.matched_node = FieldNode(str(i),
                                                          elem,
                                                          exists=True,
                                                          in_array=True,
                                                          parent=node)
                            yield elem
                if pack:
                    yield doc
            else:
                # doc or array positioned doc
                if not array_only and doc is not _no_val:
                    yield doc

        self.matched_node = None

    def iter_full(self):
        return self._iter(False, True, True)

    def iter_arrays(self):
        return self._iter(True, False, True)

    def __next__(self):
        return next(self.__iter)

    next = __next__

    def __iter__(self):
        self.__iter = self._value_iter()
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._value_iter = self.iter_full

    def __repr__(self):
        return "FieldValues({})".format(self.values)

    def __eq__(self, other):
        return self.values == other


class FieldNode(str):
    __slots__ = ("value", "located", "exists",
                 "in_array", "parent", "children")

    def __new__(cls, field, doc, located=False, exists=False,
                in_array=False, parent=None):
        obj = str.__new__(cls, field)
        obj.value = doc
        obj.located = located
        obj.exists = exists
        obj.in_array = in_array
        obj.parent = parent
        obj.children = []

        if getattr(field, "in_array", False):
            obj.in_array = True

        return obj

    def __repr__(self):
        return "FieldNode({})".format(self)

    def __iter__(self):
        return iter(self.children)

    def __contains__(self, field):
        return field in self.children

    def __len__(self):
        return len(self.children)

    def __getitem__(self, field):
        for node in self.children:
            if node == field:
                return node
        raise KeyError

    def is_missing(self):
        if self.in_array and not self.located:
            return not self.exists  # doc in array, missing if not exists
        if not self.in_array and not self.exists:
            return True
        return False

    def full_path(self):
        forepath = getattr(self.parent, "full_path", lambda: "")()
        if forepath:
            return forepath + "." + str(self)
        return str(self)

    def spawn(self, value, field, located=False, exists=True, in_array=False):
        self.children.append(
            FieldNode(field, value, located, exists, in_array, self))


class FieldTreeReader(object):

    def __init__(self, tree):
        self.map_cls = tree.map_cls

    def operate(self, node, field):
        if node.exists is False:
            return [node]

        if isinstance(node.value, self.map_cls):
            self.read_map(node, field)
        elif isinstance(node.value, list):
            self.read_array(node, field)
        else:
            node.spawn(_no_val, field, exists=False,
                       located=node.located, in_array=node.in_array)

        return node.children

    def read_map(self, node, field, index=None, elem=None):
        doc = node.value if elem is None else elem

        try:
            val = doc[field]
            exists = True
        except KeyError:
            val = _no_val
            exists = False

        if index:
            field = index + "." + field
        node.spawn(val, field, exists=exists, in_array=bool(index))

    def read_array(self, node, field):
        doc = node.value

        for i, elem in enumerate(doc):
            if isinstance(elem, self.map_cls):
                self.read_map(node, field, str(i), elem)

        if field.isdigit():
            try:
                val = doc[int(field)]
                exists = True
            except IndexError:
                val = _no_val
                exists = False
            node.spawn(val, field, located=True, exists=exists, in_array=True)


class FieldTreeWriter(object):

    def __init__(self, tree):
        self.map_cls = tree.map_cls
        self.on_delete = False

    def operate(self, node, field):
        if isinstance(node.value, self.map_cls):
            self.write_map(node, field)
        elif isinstance(node.value, list) and field.isdigit():
            self.write_array(node, field)
        elif not self.on_delete:
            msg = ("Cannot create field {0!r} in element "
                   "{1}".format(field, {str(node): node.value}))
            raise FieldCreateError(msg)

        return node.children

    def write_map(self, node, field, index=None, elem=None):
        doc = node.value if elem is None else elem

        try:
            val = doc[field]
        except KeyError:
            val = self.map_cls()

        if index:
            field = index + "." + field
        node.spawn(val, field, in_array=bool(index))

    def write_array(self, node, field):
        doc = node.value

        try:
            val = doc[int(field)]
        except IndexError:
            val = self.map_cls()

        node.spawn(val, field, located=True, in_array=True)


def is_conflict(path_a, path_b):
    return (path_a == path_b or
            path_a.startswith(path_b + ".") or
            path_b.startswith(path_a + "."))


class FieldTree(object):

    def __init__(self, doc, doc_type=None):
        self.map_cls = doc_type or type(doc)
        self.root = FieldNode("", doc, exists=True)
        self.handler = None
        self.picked = None
        self.changes = list()

    def __str__(self):
        def print_tree(node, level=0):
            status = "*" * ((not node.exists) + node.is_missing())
            tree_str = "\t" * level + node + status + "{}\n"

            if len(node) == 0:
                tree_str = tree_str.format(": " + str(node.value))
            else:
                tree_str = tree_str.format("")

            for child in node:
                tree_str += print_tree(child, level + 1)

            return tree_str

        return "FieldTree({})".format(print_tree(self.root))

    def __repr__(self):
        return "FieldTree({})".format(self)

    def clear(self):
        self.root.children = list()

    def grow(self, fields):
        self.picked = [self.root]
        pre_field = ""

        for field in fields:
            old_picked = []
            for node in [child for node in self.picked for child in node]:
                if field == node or node.endswith("." + field):
                    old_picked.append(node)

            if old_picked:
                self.picked = old_picked
                pre_field = field
                continue

            new_picked = []
            for node in self.picked:
                if not (pre_field == node or node.endswith("." + pre_field)):
                    continue

                new_picked += self.handler.operate(node, field)

            self.picked = new_picked
            pre_field = field

            # When READ, stop if all nodes not exists
            if (isinstance(self.handler, FieldTreeReader) and
                    all(node.exists is False for node in self.picked)):
                break

    def read(self, fields):
        self.handler = FieldTreeReader(self)
        self.grow(fields)
        return FieldValues(self.picked)

    def stage(self, field, value, evaluator=None):
        """Internal method, for staging the changes
        """
        evaluator = evaluator or (lambda _, val: val)
        staging = [node for node in self.picked if node == field]
        updates = list()

        for node in staging:
            update = node.full_path()
            for changed in self.changes:
                if is_conflict(update, changed):
                    msg = ("Updating the path {0!r} would create a conflict "
                           "at {1!r}".format(update, changed))
                    raise FieldConflictError(msg)

            node.value = evaluator(node.value, value)
            updates.append(update)

        self.changes += updates

    def write(self, fields, value, evaluator=None):
        self.handler = FieldTreeWriter(self)
        self.handler.on_delete = value is _no_val
        self.grow(fields)
        self.stage(fields[-1], value, evaluator=evaluator)

    def delete(self, fields):
        self.write(fields, _no_val)

    def extract(self):
        ON_DELETE = False
        if isinstance(self.handler, FieldTreeWriter):
            ON_DELETE = self.handler.on_delete

        def _extract(node):
            doc = node.value

            if isinstance(doc, self.map_cls):
                new_doc = self.map_cls()
                fields = list(doc.keys()) + [str(child) for child in node
                                             if child not in doc]
                for field in fields:
                    try:
                        child = node[field]
                    except KeyError:
                        value = doc[field]
                    else:
                        value = _extract(child)
                    finally:
                        if value is not _no_val:
                            new_doc[field] = value

                return new_doc

            elif isinstance(doc, list):
                new_doc = list()
                indices = range(max(
                    [len(doc)] + [int(n) + 1 for n in node if n.isdigit()]))

                for index in indices:
                    if ON_DELETE and index >= len(doc):
                        continue

                    try:
                        child = node[str(index)]
                    except KeyError:
                        value = doc[index] if index < len(doc) else None
                    else:
                        value = _extract(child)
                    finally:
                        if value is _no_val:
                            value = None
                        new_doc.append(value)

                return new_doc

            else:
                return doc

        return _extract(self.root)


class FieldWalker(object):
    """Document traversal context manager

    __slots__ = (
        "doc",
        "doc_type",
        "value",
        "log",
    )
    """

    def __init__(self, doc, doc_type=None):
        """
        """
        self.doc = doc
        self.doc_type = doc_type or type(doc)
        self.steps = None
        self.tree = FieldTree(doc, doc_type)
        self.value = None

    def go(self, path):
        self.steps = tuple(path.split("."))
        return self

    def get(self):
        """Walk through document and acquire value with given key-path
        """
        self.value = self.tree.read(self.steps)
        return self

    def set(self, value, evaluator=None):
        self.tree.write(self.steps, value, evaluator)

    def drop(self):
        self.tree.delete(self.steps)

    def commit(self):
        return self.tree.extract()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.tree.clear()
