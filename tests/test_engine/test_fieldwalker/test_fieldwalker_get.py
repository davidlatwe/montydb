from montydb.engine.field_walker import FieldWalker


def test_fieldwalker_value_get_1():
    # single value
    doc = {"a": 1}
    path = "a"
    value = [1]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_2():
    doc = {"a": {"b": 1}}
    path = "a"
    value = [{"b": 1}]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_3():
    doc = {"a": {"b": 1}}
    path = "a.b"
    value = [1]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_4():
    # array value
    doc = {"a": [1]}
    path = "a"
    value = [1, [1]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_5():
    doc = {"a": [0, 1]}
    path = "a"
    value = [0, 1, [0, 1]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_6():
    doc = {"a": [0, [1], 2]}
    path = "a"
    value = [0, [1], 2, [0, [1], 2]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_7():
    doc = {"a": {"b": [1]}}
    path = "a.b"
    value = [1, [1]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_8():
    doc = {"a": [{"b": [0, 1]}, {"b": [2, 3]}, {"b": [4, 5]}]}
    path = "a.b"
    value = [0, 1, [0, 1], 2, 3, [2, 3], 4, 5, [4, 5]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_9():
    # array opsitioned
    doc = {"a": {"b": [1]}}
    path = "a.b.0"
    value = [1]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_10():
    doc = {"a": [0, 1]}
    path = "a.1"
    value = [1]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_11():
    doc = {"a": [{"1": 0}, 1]}
    path = "a.0"
    value = [{"1": 0}]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_12():
    doc = {"a": [[0, 1], 1]}
    path = "a.0.1"
    value = [1]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_13():
    doc = {"a": [{"1": 0}, 1]}
    path = "a.0.1"
    value = [0]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_14():
    doc = {"a": [{"b": [0, 1]}, {"b": [2, 3]}, {"b": [4, 5]}]}
    path = "a.b.1"
    value = [1, 3, 5]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_15():
    # array opsitioned and digit-str field
    doc = {"a": [{"1": 0}, 1]}
    path = "a.1"
    value = [0, 1]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_16():
    doc = {"a": [{"1": 0}, 1, {"1": 2}, 3, {"1": 4}]}
    path = "a.1"
    value = [0, 2, 4, 1]  # notice that doc values are before array element
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_17():
    doc = {"a": [{"b": [0, 1, {"1": 99}]},
                 {"b": [2, 3]}]}
    path = "a.b.1"
    value = [99, 1, 3]  # doc value (99) are before array elements (1, 3)
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_18():
    # array opsitioned and embedded documents
    doc = {"a": [{"b": [0, {"c": 1}]},
                 {"b": [2, {"c": 3}]},
                 {"b": [4, {"c": 5}]}]}
    path = "a.b.1.c"
    value = [1, 3, 5]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_19():
    doc = {"a": [{"b": [0, {"c": [1, "x"]}]},
                 {"b": [2, {"c": [3, "y"]}]},
                 {"b": [4, {"c": [5, "z"]}]}]}
    path = "a.b.1.c"
    value = [1, "x", [1, "x"], 3, "y", [3, "y"], 5, "z", [5, "z"]]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_20():
    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d"
    value = [
        1, "x", [1, "x"], 3, "y", [3, "y"], 5, "z", [5, "z"],
        11, "i", [11, "i"], 13, "j", [13, "j"], 15, "k", [15, "k"]
    ]
    field_value = FieldWalker(doc).go(path).get().value
    assert list(field_value.iter_full()) == value


def test_fieldwalker_value_get_21():
    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "y", "z", "i", "j", "k"]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_22():
    # array opsitioned and embedded documents and digit-str field
    doc = {"a": [{"b": [0, {"c": 1}, {"1": 99}]},
                 {"b": [2, {"c": 3}]}]}
    path = "a.b.1.c"
    value = [1, 3]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_23():
    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}, {"1": 99}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "y", "z", "i", "j", "k"]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_24():
    # with missing field
    doc = {"a": [{"X": [{"c": [0, {"d": [1, "x"]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["i", "j", "k"]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_25():
    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"X": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "z", "i", "j", "k"]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_26():
    # array element shortage
    doc = {"a": [{"b": [{"c": [0, {"d": [1]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["y", "z", "i", "j", "k"]
    assert FieldWalker(doc).go(path).get().value == value


def test_fieldwalker_value_get_27():
    doc = {"a": [{"b": [{"c": [{"0": [{"d": [0, 1]}]}, {"d": [1]}]},
                        {"c": [{"0": [{"d": [0, 2]}]}, {"d": [3, "y"]}]},
                        {"c": [{"d": [5, "z"]}, {"0": [{"d": [0, 3]}]}]}]},
                 {"b": [{"c": [{"0": [{"d": [0, 4]}]}, {"d": [11, "i"]}]},
                        {"c": [{"0": [{"d": [0, 5]}]}, {"d": [13, "j"]}]},
                        {"c": [{"0": [{"d": [0, 6]}]}, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.0.d.1"
    value = [1, 2, 3, "z", 4, 5, 6]
    fw = FieldWalker(doc).go(path).get()
    assert fw.value == value


def test_fieldwalker_value_get_28():
    doc = {"a": [{"1": {"b": 5}}, 1]}
    path = "a.1.b"
    value = [5]
    fw = FieldWalker(doc).go(path).get()
    assert fw.value == value


def test_fieldwalker_value_get_29():
    doc = {"a": [True, True, {"2": True, "3": True}]}
    path = "a.3"
    value = [True]
    fw = FieldWalker(doc).go(path).get()
    assert fw.value == value


def test_fieldwalker_clean_result_1():
    doc = {"a": 5, "b": 8}

    fieldwalker = FieldWalker(doc)

    with fieldwalker:
        fieldwalker.go("b").get()
        fieldwalker.go("a").get()

        assert fieldwalker.value == [5]
