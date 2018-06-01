from montydb.engine.core import FieldWalker


def test_fieldwalker_value_retrieve():

    # single value

    doc = {"a": 1}
    path = "a"
    value = [1]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": {"b": 1}}
    path = "a"
    value = [{"b": 1}]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": {"b": 1}}
    path = "a.b"
    value = [1]
    assert FieldWalker(doc)(path).value == value

    # array value

    doc = {"a": [1]}
    path = "a"
    value = [1, [1]]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [0, 1]}
    path = "a"
    value = [0, 1, [0, 1]]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [0, [1], 2]}
    path = "a"
    value = [0, [1], 2, [0, [1], 2]]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": {"b": [1]}}
    path = "a.b"
    value = [1, [1]]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [0, 1]}, {"b": [2, 3]}, {"b": [4, 5]}]}
    path = "a.b"
    value = [0, 1, 2, 3, 4, 5, [0, 1], [2, 3], [4, 5]]
    assert FieldWalker(doc)(path).value == value

    # array opsitioned

    doc = {"a": {"b": [1]}}
    path = "a.b.0"
    value = [1]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [0, 1]}
    path = "a.1"
    value = [1]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"1": 0}, 1]}
    path = "a.0"
    value = [{"1": 0}]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [[0, 1], 1]}
    path = "a.0.1"
    value = [1]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"1": 0}, 1]}
    path = "a.0.1"
    value = [0]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [0, 1]}, {"b": [2, 3]}, {"b": [4, 5]}]}
    path = "a.b.1"
    value = [1, 3, 5]
    assert FieldWalker(doc)(path).value == value

    # array opsitioned and digit-str field

    doc = {"a": [{"1": 0}, 1]}
    path = "a.1"
    value = [0, 1]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"1": 0}, 1, {"1": 2}, 3, {"1": 4}]}
    path = "a.1"
    value = [0, 2, 4, 1]  # notice that doc values are before array element
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [0, 1, {"1": 99}]},
                 {"b": [2, 3]}]}
    path = "a.b.1"
    value = [99, 1, 3]  # doc value (99) are before array elements (1, 3)
    assert FieldWalker(doc)(path).value == value

    # array opsitioned and embedded documents

    doc = {"a": [{"b": [0, {"c": 1}]},
                 {"b": [2, {"c": 3}]},
                 {"b": [4, {"c": 5}]}]}
    path = "a.b.1.c"
    value = [1, 3, 5]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [0, {"c": [1, "x"]}]},
                 {"b": [2, {"c": [3, "y"]}]},
                 {"b": [4, {"c": [5, "z"]}]}]}
    path = "a.b.1.c"
    value = [1, "x", 3, "y", 5, "z", [1, "x"], [3, "y"], [5, "z"]]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d"
    value = [
        1, "x", 3, "y", 5, "z", 11, "i", 13, "j", 15, "k",
        [1, "x"], [3, "y"], [5, "z"], [11, "i"], [13, "j"], [15, "k"]
    ]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "y", "z", "i", "j", "k"]
    assert FieldWalker(doc)(path).value == value

    # array opsitioned and embedded documents and digit-str field

    doc = {"a": [{"b": [0, {"c": 1}, {"1": 99}]},
                 {"b": [2, {"c": 3}]}]}
    path = "a.b.1.c"
    value = [1, 3]
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}, {"1": 99}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "y", "z", "i", "j", "k"]
    assert FieldWalker(doc)(path).value == value

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
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [{"c": [0, {"d": [1, "x"]}]},
                        {"X": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.1"
    value = ["x", "z", "i", "j", "k"]
    assert FieldWalker(doc)(path).value == value

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
    assert FieldWalker(doc)(path).value == value

    doc = {"a": [{"b": [{"c": [{"0": [{"d": [0, 1]}]}, {"d": [1]}]},
                        {"c": [{"0": [{"d": [0, 2]}]}, {"d": [3, "y"]}]},
                        {"c": [{"d": [5, "z"]}, {"0": [{"d": [0, 3]}]}]}]},
                 {"b": [{"c": [{"0": [{"d": [0, 4]}]}, {"d": [11, "i"]}]},
                        {"c": [{"0": [{"d": [0, 5]}]}, {"d": [13, "j"]}]},
                        {"c": [{"0": [{"d": [0, 6]}]}, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.0.d.1"
    value = [1, 2, 3, 4, 5, 6, "z"]
    fw = FieldWalker(doc)(path)
    assert fw.value == value
    assert fw.index_posed is True
    assert fw.embedded_in_array is True

    doc = {"a": [{"1": {"b": 5}}, 1]}
    path = "a.1.b"
    value = [5]
    fw = FieldWalker(doc)(path)
    assert fw.value == value
    assert fw.index_posed is False
    assert fw.embedded_in_array is True

    doc = {"a": [True, True, {"2": True, "3": True}]}
    path = "a.3"
    value = [True]
    fw = FieldWalker(doc)(path)
    assert fw.value == value


def test_fieldwalker_value_set():
    doc = {"a": 5}
    path = "a"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": 10}

    doc = {"a": [5, 8]}
    path = "a.1"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [5, 10]}

    doc = {"a": [{"b": 5}, {"b": 6}]}
    path = "a.b"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": 10}, {"b": 10}]}

    doc = {"a": {"b": [5, 8]}}
    path = "a.b.1"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": {"b": [5, 10]}}

    doc = {"a": [{"b": [5, 8]}, {"b": [6, 9]}]}
    path = "a.b.1"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": [5, 10]}, {"b": [6, 10]}]}

    doc = {"a": [{"b": [{"c": [0, {"d": [1]}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.2"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": [{"c": [0, {"d": [1, None, 10]}]},
                                {"c": [2, {"d": [3, "y", 10]}]},
                                {"c": [4, {"d": [5, "z", 10]}]}]},
                         {"b": [{"c": [10, {"d": [11, "i", 10]}]},
                                {"c": [12, {"d": [13, "j", 10]}]},
                                {"c": [14, {"d": [15, "k", 10]}]}]}
                         ]}

    doc = {"a": [{"b": [{"c": [0, {"d": {}}]},
                        {"c": [2, {"d": [3, "y"]}]},
                        {"c": [4, {"d": [5, "z"]}]}]},
                 {"b": [{"c": [10, {"d": [11, "i"]}]},
                        {"c": [12, {"d": [13, "j"]}]},
                        {"c": [14, {"d": [15, "k"]}]}]}
                 ]}
    path = "a.b.c.1.d.2"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": [{"c": [0, {"d": {"2": 10}}]},
                                {"c": [2, {"d": [3, "y", 10]}]},
                                {"c": [4, {"d": [5, "z", 10]}]}]},
                         {"b": [{"c": [10, {"d": [11, "i", 10]}]},
                                {"c": [12, {"d": [13, "j", 10]}]},
                                {"c": [14, {"d": [15, "k", 10]}]}]}
                         ]}

    doc = {"a": [{"b": "doc"}, {"0": ["doc"]}]}
    path = "a.b.1"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": "doc"}, {"0": ["doc"]}]}

    doc = {"a": [{"b": "doc"}, {"0": ["doc"]}]}
    path = "a.0.b"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": 10}, {"0": ["doc"]}]}

    doc = {"a": [{"b": "doc"}, {"0": {"b": ["doc", "x"]}}]}
    path = "a.0.b"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"b": 10}, {"0": {"b": 10}}]}

    doc = {"a": [{"0": None}]}
    path = "a.0.b"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [{"0": None}]}

    doc = {"a": [True, {"1": None}, {"1": False, "0": False}]}
    path = "a.1.2"
    fw = FieldWalker(doc)(path)
    fw.setval(10)
    assert doc == {"a": [True, {"1": None}, {"1": False, "0": False}]}
