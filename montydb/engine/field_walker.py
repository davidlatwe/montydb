class _NoVal(object):
    def __repr__(self):
        return "_NoVal()"
    __slots__ = ()


# Value of missing field
_no_val = _NoVal()


class FieldWalkError(Exception):
    """Base class for FieldWalker exceptions."""


class FieldWriteError(FieldWalkError):
    """FieldWalker write operation error class"""

    def __init__(self, error, code=None):
        self.__code = code
        super(FieldWriteError, self).__init__(error)

    @property
    def code(self):
        return self.__code


class FieldValues(object):
    """Document field status and values iterator

    Store document tree nodes that matched from query, and iterate nodes values
    by interests. Also compute document field status from input nodes.

    Arguments:
        nodes (list): A list of nodes that picked from `FieldTree`
        fieldwalker: Instance of `FieldWalker` which made the query

    """

    __slots__ = (
        "nodes",
        "_is_exists",
        "_null_or_missing",
        "_fieldwalker",
        "_value_iter",
        "__iter",
    )

    def __init__(self, nodes, fieldwalker):
        self.nodes = nodes
        self._fieldwalker = fieldwalker

        self._is_exists = None
        self._null_or_missing = None

        self._value_iter = self.iter_full

    def is_exists(self):
        """Is field exists ?"""
        if self._is_exists is None:
            is_exists = any(nd.exists for nd in self.nodes)
            # Cache
            self._is_exists = is_exists

            return is_exists
        else:
            return self._is_exists

    def null_or_missing(self):
        """Is None value or missing field ?"""
        if self._null_or_missing is None:
            null_or_missing = (
                any(nd.is_missing() for nd in self.nodes)
                or self.is_exists()
                and None in self.iter_flat()
            )
            # Cache
            self._null_or_missing = null_or_missing

            return null_or_missing
        else:
            return self._null_or_missing

    def _iter(self, array_only, unpack, pack):
        """Value iterator

        Args:
            array_only (bool): Yield only `list` type values.
            unpack (bool): If value is a `list`, yield it's elements.
            pack (bool): If value is a `list`, yield it.

        """
        fieldwalker = self._fieldwalker
        for node in self.nodes:
            fieldwalker.put_matched(node)

            value = node.value
            if isinstance(value, list):
                # value in array
                if unpack and (array_only or not node.located):
                    for i, elem in enumerate(value):
                        if elem is not _no_val:
                            matched = FieldNode(
                                str(i), elem, exists=True, in_array=True, parent=node
                            )
                            fieldwalker.put_matched(matched)
                            yield elem
                if pack:
                    # Include whole array as part of query result
                    yield value
            else:
                # Value of document field or value of positioned array element
                if not array_only and value is not _no_val:
                    yield value

        # Reset to `None` if the iter loop did not *break* in query
        fieldwalker.put_matched(None)

    def iter_plain(self):
        """Iterate each nodes value as is"""
        return self._iter(array_only=False, unpack=False, pack=True)

    def iter_flat(self):
        """Iterate each nodes value and unpack list value's element"""
        return self._iter(array_only=False, unpack=True, pack=False)

    def iter_full(self):
        """Iterate each nodes value as is, also unpack list value's element"""
        return self._iter(array_only=False, unpack=True, pack=True)

    def iter_arrays(self):
        """Only iterate list type values"""
        return self._iter(array_only=True, unpack=False, pack=True)

    def iter_elements(self):
        """Only iterate list type values' elements"""
        return self._iter(array_only=True, unpack=True, pack=False)

    def change_iter(self, func):
        """Change value iterator

        func: A function that returns an iterator

        """
        self._value_iter = func

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
        return "FieldValues({})".format(self.iter_plain())

    def __eq__(self, other):
        return list(self.iter_plain()) == other


class FieldNode(str):
    """Document field node

    Arguments:
        field (str): Document field name
        doc: field value
        located (bool): Is this a positioned field (array element)
        exists (bool): Is this field exists
        in_array (bool): Is this document inside an array
        parent (FieldNode): Parent node

    """

    __slots__ = ("value", "located", "exists", "full_path",
                 "in_array", "parent", "children")

    def __new__(
        cls, field, doc, located=False, exists=False, in_array=False, parent=None
    ):
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

    def __init__(self, *args, **kwargs):
        super(FieldNode, self).__init__()
        self.full_path = self.concat_parents()

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

    def concat_parents(self):
        forepath = getattr(self.parent, "concat_parents", lambda: "")()
        if forepath:
            return forepath + "." + str(self)
        return str(self)

    def spawn(self, value, field, located=False, exists=True, in_array=False):
        new_node = FieldNode(field, value, located, exists, in_array, self)
        self.children.append(new_node)
        return new_node


class FieldTreeReader(object):
    def __init__(self, tree):
        self.map_cls = tree.map_cls
        self.trace = set()

    def operate(self, node, field):
        if node.exists is False:
            return [node]

        result = list()

        if isinstance(node.value, self.map_cls):
            result.append(self.read_map(node, field))
        elif isinstance(node.value, list):
            result += self.read_array(node, field)
        else:
            new_node = node.spawn(
                _no_val,
                field,
                exists=False,
                located=node.located,
                in_array=node.in_array,
            )
            result.append(new_node)

        return result

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

        self.trace.add(field)
        return node.spawn(val, field, exists=exists, in_array=bool(index))

    def read_array(self, node, field):
        new_nodes = list()
        doc = node.value

        for i, elem in enumerate(doc):
            if isinstance(elem, self.map_cls):
                new_nodes.append(self.read_map(node, field, str(i), elem))

        if field.isdigit():
            try:
                val = doc[int(field)]
                exists = True
            except IndexError:
                val = _no_val
                exists = False

            self.trace.add(field)
            new_nodes.append(
                node.spawn(val, field, located=True, exists=exists, in_array=True)
            )
        return new_nodes


def is_multi_position_operator(field):
    return field[:2] == "$["


def parse_identifier(field):
    return field[2:-1]  # $[identifier]


_dollar_prefixed_err_msg = (
    "The dollar ($) prefixed field {0!r} in {1!r} is not valid for storage."
)


def no_dollar_prefix_field(doc, map_cls, root_path):
    if isinstance(doc, map_cls):
        for field, value in doc.items():
            if field.startswith("$"):
                full_path = root_path + "." + field
                msg = _dollar_prefixed_err_msg.format(field, full_path)
                raise FieldWriteError(msg, code=52)

            no_dollar_prefix_field(value, map_cls, root_path + "." + field)

    if isinstance(doc, list):
        for i, elem in enumerate(doc):
            no_dollar_prefix_field(elem, map_cls, root_path + "." + str(i))


class FieldTreeWriter(object):
    def __init__(self, tree):
        self.map_cls = tree.map_cls
        self.filters = None
        self.on_delete = False
        self.trace = set()

    def operate(self, node, field):
        result = list()

        if not field:
            raise FieldWriteError("An empty update path is not valid", code=56)

        if field.startswith("."):
            msg = (
                "The update path {} contains an empty field name, which is "
                "not allowed".format(field)
            )
            raise FieldWriteError(msg, code=56)

        if field.startswith("$") and not field.startswith("$["):
            full_path = node.full_path + "." + field
            msg = _dollar_prefixed_err_msg.format(field, full_path)
            raise FieldWriteError(msg, code=52)

        if not node.exists and is_multi_position_operator(field):
            msg = (
                "The path {0!r} must exist in the document in order "
                "to apply array updates.".format(node.full_path)
            )
            raise FieldWriteError(msg, code=2)

        if is_multi_position_operator(field) and not isinstance(node.value, list):
            msg = "Cannot apply array updates to non-array element {0}: {1}".format(
                str(node), node.value
            )
            raise FieldWriteError(msg, code=2)

        if isinstance(node.value, self.map_cls):

            result.append(self.write_map(node, field))

        elif isinstance(node.value, list) and (
            field.isdigit() or is_multi_position_operator(field)
        ):

            result += self.write_array(node, field)

        elif not self.on_delete:
            msg = "Cannot create field {0!r} in element {1}".format(
                field, {str(node): node.value}
            )
            raise FieldWriteError(msg, code=28)

        return result

    def write_map(self, node, field, index=None, elem=None):
        doc = node.value if elem is None else elem

        try:
            val = doc[field]
            exists = True
        except KeyError:
            val = self.map_cls()
            exists = False

        if index:
            field = index + "." + field

        self.trace.add(field)
        return node.spawn(val, field, exists=exists, in_array=bool(index))

    def write_array(self, node, field):
        new_nodes = list()
        doc = node.value

        if is_multi_position_operator(field):
            identifier = parse_identifier(field)
            if identifier:
                # filter
                filter = self.filters[identifier]
                for i, elem in enumerate(doc):
                    if filter({identifier: elem}):
                        field = str(i)

                        self.trace.add(field)
                        new_nodes.append(
                            node.spawn(
                                elem, field, located=True, exists=True, in_array=True
                            )
                        )
            else:
                # all
                for i, elem in enumerate(doc):
                    field = str(i)

                    self.trace.add(field)
                    new_nodes.append(
                        node.spawn(
                            elem, field, located=True, exists=True, in_array=True
                        )
                    )
        else:
            try:
                val = doc[int(field)]
                exists = True
            except IndexError:
                val = self.map_cls()
                exists = False

            self.trace.add(field)
            new_nodes.append(
                node.spawn(val, field, located=True, exists=exists, in_array=True)
            )
        return new_nodes


def is_conflict(path_a, path_b):
    return (
        path_a == path_b
        or path_a.startswith(path_b + ".")
        or path_b.startswith(path_a + ".")
    )


class FieldTree(object):
    def __init__(self, doc, doc_type=None):
        self.map_cls = doc_type or type(doc)
        self.root = FieldNode("", doc, exists=True)
        self.handler = None
        self.changes = None
        self.picked = None
        self.previous = None
        self.clear()

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
        self.handler = None
        self.changes = list()
        self.restart()

    def restart(self):
        self.picked = [self.root]
        self.previous = {""}

    def grow(self, fields):

        for field in fields:

            if not is_multi_position_operator(field):
                # Reuse previous spawned nodes
                old_picked = []
                for node in [child for node in self.picked for child in node]:
                    if field == node and node.parent in self.previous:
                        old_picked.append(node)

                if old_picked:
                    self.picked = old_picked
                    self.previous.add(field)
                    continue

            self.handler.trace.clear()
            new_picked = []
            for node in self.picked:
                if node not in self.previous:
                    continue

                new_picked += self.handler.operate(node, field)

            self.picked = new_picked

            self.previous = self.handler.trace.copy()

            # When READ, stop if all nodes not exists
            if isinstance(self.handler, FieldTreeReader) and all(
                node.exists is False for node in self.picked
            ):
                break

    def read(self, fields):
        self.handler = FieldTreeReader(self)
        self.grow(fields)
        return self.picked

    def stage(self, value, evaluator=None):
        """Internal method, for staging the changes"""
        evaluator = evaluator or (lambda _, val: val)
        staging = [node for node in self.picked if node in self.previous]
        updates = list()

        for node in staging:
            update = node.full_path
            for changed in self.changes:
                if is_conflict(update, changed):
                    msg = (
                        "Updating the path {0!r} would create a conflict "
                        "at {1!r}".format(update, changed)
                    )
                    raise FieldWriteError(msg, code=40)

            new_value = evaluator(node, value)

            if node.value != new_value:
                no_dollar_prefix_field(new_value, self.map_cls, node.full_path)
                node.value = new_value
                updates.append(update)

        self.changes += updates

    def fields_positioning(self, fieldwalker, array_filters=None):
        fields = fieldwalker.steps

        if "$" in fields:

            if not fieldwalker.has_matched():
                # If hasn't queried or not matched in array
                msg = (
                    "The positional operator did not find the match needed "
                    "from the query."
                )
                raise FieldWriteError(msg, code=2)

            elif fields.count("$") > 1:
                msg = (
                    "Too many positional (i.e. '$') elements found in path "
                    "{!r}".format(".".join(fields))
                )
                raise FieldWriteError(msg, code=2)

            else:
                # Replace "$" into matched array element index
                matched = fieldwalker.get_matched()
                position = matched.split(".")[0]
                fields[fields.index("$")] = position

        if array_filters is None:
            return fields

        for field in fields:
            if is_multi_position_operator(field):
                identifier = parse_identifier(field)

                if identifier and identifier not in array_filters:
                    msg = (
                        "No array filter found for identifier {0!r} in "
                        "path {1!r}".format(identifier, ".".join(fields))
                    )
                    raise FieldWriteError(msg, code=2)
        return fields

    def write(self, fields, value, evaluator=None, array_filters=None):
        self.handler = FieldTreeWriter(self)
        self.handler.filters = array_filters
        self.handler.on_delete = value is _no_val
        self.grow(fields)
        self.stage(value, evaluator=evaluator)

    def delete(self, fields, array_filters=None):
        self.write(fields, _no_val, array_filters=array_filters)

    def extract(self, visited_only=False):
        ON_DELETE = False
        if isinstance(self.handler, FieldTreeWriter):
            ON_DELETE = self.handler.on_delete

        def _extract(node, visited_only_):
            doc = node.value
            has_children = bool(node.children)
            _visited_only = visited_only_

            if isinstance(doc, self.map_cls):
                new_doc = self.map_cls()
                fields = list(doc.keys()) + [
                    str(child) for child in node if child not in doc
                ]
                for field in fields:
                    try:
                        child = node[field]
                    except KeyError:
                        if visited_only_ and has_children:
                            _visited_only = False
                            continue
                        value = doc[field]
                    else:
                        value = _extract(child, _visited_only)

                    if value is not _no_val:
                        new_doc[field] = value

                return new_doc

            elif isinstance(doc, list):
                new_doc = list()
                indices = range(
                    max([len(doc)] + [int(n) + 1 for n in node if n.isdigit()])
                )

                for index in indices:
                    if ON_DELETE and index >= len(doc):
                        continue

                    try:
                        child = node[str(index)]
                    except KeyError:
                        if visited_only_ and has_children:
                            _visited_only = False
                            continue
                        value = doc[index] if index < len(doc) else None
                    else:
                        value = _extract(child, _visited_only)

                    if value is _no_val:
                        value = None
                    new_doc.append(value)

                return new_doc

            else:
                return doc

        return _extract(self.root, visited_only)


class FieldWalker(object):
    """Document field traversal interface for MontyDB

    Key component to make everything run. Every document operation in MontyDB
    use this to read/write the field value into document or to delete field.

    The `FieldWalker` does not interact with the input document directly, it
    initialized with a tree (`FieldTree`), and adding nodes (`FieldNode`) via
    the input field.

    when read, all matched fields' values will be carry out by a `FieldValues`
    instance.

    When write/delete, you need to commit the changes before it take effect.

    Example:
        >>> from montydb.engine.field_walker import FieldWalker
        >>> doc = {"name": "Tom", "detail": {"age": None, "weight": 80}}
        >>> walker = FieldWalker(doc)
        >>> walker.go("name").get().value
        FieldValues(['Tom'])
        >>> walker.go("detail.weight").get().value
        FieldValues([80])
        >>> walker.go("detail.age").set(30)
        >>> walker.commit()
        True
        >>> walker.doc
        {'name': 'Tom', 'detail': {'age': 30, 'weight': 80}}

    Arguments:
        doc: The document to operate on. Must be a mutable mapping type.
        doc_type (optional): The document class, specify what type of the
            document is. If `doc_type` not provided, will use `type(doc)`
            to get document class.

    """

    def __init__(self, doc, doc_type=None):
        self.doc = doc
        self.doc_type = doc_type or type(doc)
        self.steps = None
        self.tree = FieldTree(doc, doc_type)
        self.value = None
        self.path = None
        self.matched = dict()

    def go(self, path):
        """Input document traversing field path

        https://docs.mongodb.com/manual/core/document/#dot-notation

        Arguments:
            path (str): Document field path

        Returns:
            (FieldWalker): self

        """
        self.tree.restart()
        self.path = path
        self.steps = path.split(".")
        return self

    def step(self, field):
        """Push traversing one depth further

        Arguments:
            field (str): Document field name

        Returns:
            (FieldWalker): self

        """
        if self.path is None:
            self.path = field
        else:
            self.path += "." + field
        self.steps = [field]
        return self

    def restart(self):
        """Reset traversing state

        Returns:
            (FieldWalker): self

        """
        self.tree.restart()
        self.path = None
        return self

    def get(self):
        """Read all values from previous given field path

        Returns:
            (FieldWalker): self

        """
        self.value = FieldValues(self.tree.read(self.steps), self)
        return self

    def set(self, value, evaluator=None, array_filters=None):
        """Write value into fields that matched

        This operation will not take effect util `commit()`.

        Arguments:
            value: Any value that needs to be written into
            evaluator (optional): A function which compute the result from
                `value` and the current field value from document, and the
                result will be the new value to write into document.
                This function should take two arguments, the first arg is an
                instance of `.engine.core.field_walker.FieldNode`, the second
                is the `value`.
            array_filters (optional): An array filter document which formed as
                `{<top-field-name>: <.engine.queries.QueryFilter>, ..}`.

        """
        steps = self.tree.fields_positioning(self, array_filters)
        self.tree.write(steps, value, evaluator, array_filters)

    def drop(self, array_filters=None):
        """Delete field

        This operation will not take effect util `commit()`.

        Arguments:
            array_filters (optional): An array filter document which formed as
                `{<top-field-name>: <.engine.queries.QueryFilter>, ..}`.

        """
        steps = self.tree.fields_positioning(self, array_filters)
        self.tree.delete(steps, array_filters)

    def commit(self):
        """Update document if any change been staged

        Returns:
            bool: Return `True` if any change happened, else `False`

        """
        has_change = bool(self.tree.changes)
        if has_change:
            self.doc = self.tree.extract()
        return has_change

    def touched(self):
        """Return a copy of document that contains only visited fields

        Returns:
            document

        """
        return self.tree.extract(visited_only=True)

    def get_matched(self, position_path=None):
        for path, node in self.matched.items():
            if position_path is None or path.startswith(position_path + "."):
                matched = node
                break
        else:
            return

        first = None
        while matched.parent is not None:
            if not matched.in_array:
                break
            first = matched
            matched = matched.parent

        return first

    def has_matched(self):
        return any(node.in_array for node in self.matched.values())

    def put_matched(self, node):
        if node is None:
            self.matched.pop(self.path, None)
        else:
            self.matched[self.path] = node

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.tree.clear()
