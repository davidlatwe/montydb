
def order_ignored_eq(a, b):
    # mongodb 4.0+ return distinct with value sorted
    assert len(a) == len(b)
    for elem in a:
        assert elem in b
    return True


def test_distinct_1(monty_distinct, mongo_distinct):
    docs = [
        {"b": 1},
        {"b": 2},
    ]
    key = "b"
    filter = None

    monty_dist = monty_distinct(docs, key, filter)
    mongo_dist = mongo_distinct(docs, key, filter)

    assert len(mongo_dist) == 2
    assert len(monty_dist) == len(mongo_dist)
    assert monty_dist == mongo_dist


def test_distinct_2(monty_distinct, mongo_distinct):
    docs = [
        {"b": 1},
        {"b": 2},
    ]
    key = "x"
    filter = None

    monty_dist = monty_distinct(docs, key, filter)
    mongo_dist = mongo_distinct(docs, key, filter)

    assert len(mongo_dist) == 0
    assert len(monty_dist) == len(mongo_dist)
    assert monty_dist == mongo_dist


def test_distinct_3(monty_distinct, mongo_distinct):
    docs = [
        {"d": 1},
        {"d": 5},
        {"d": 6},
        {"d": 0},
        {"d": 8},
    ]

    key = "d"
    filter = None

    monty_dist = monty_distinct(docs, key, filter)
    mongo_dist = mongo_distinct(docs, key, filter)

    assert len(mongo_dist) == 5
    assert len(monty_dist) == len(mongo_dist)

    assert order_ignored_eq(mongo_dist, [1, 5, 6, 0, 8])
    assert order_ignored_eq(monty_dist, mongo_dist)


def test_distinct_4(monty_distinct, mongo_distinct):
    docs = [
        {"d": [{"x": 3}, {"x": 2}]},
        {"d": [{"x": [4, {"o": "p"}, [5, 6], True]},
               {"x": [9, 2, [8], {"m": "n"}]}]},
        {"d": [{"x": 1}, {"x": 10}]},
        {"d": [{"x": [2, [3]]}, {"x": 6}]},
    ]

    key = "d.x"
    filter = None

    monty_dist = monty_distinct(docs, key, filter)
    mongo_dist = mongo_distinct(docs, key, filter)

    assert len(mongo_dist) == 13
    assert len(monty_dist) == len(mongo_dist)

    assert order_ignored_eq(mongo_dist, [
        2, 3,
        4, 9, {"m": "n"}, {"o": "p"}, [5, 6], [8], True,
        1, 10,
        6, [3]
    ])
    assert order_ignored_eq(monty_dist, mongo_dist)
