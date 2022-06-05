from swill.config import Config, Partial


def test_config_from_env(osenv):
    osenv["SWILL_SWILL__INTROSPECTION"] = "1"
    osenv["SWILL_TEST_VALUE"] = "6"

    c = Config("/")
    c.load_from_env()
    assert c["swill"]["introspection"] == True  # Technically 'introspection' is 1 here
    assert c["test_value"] == 6


def test_config_from_object():

    default_config = {
        "swill": {
            "introspection": True,
        },
        "sql": {"databases": {"test": "mysql://"}},
    }

    class MyObject:
        swill: Partial = {"prefix": "/test"}
        sql = {"databases": {"login": "postgres://"}, "charset": "utf-8"}

    c = Config("/", default_config)
    c.load_from_object(MyObject())

    assert c["swill.introspection"] is True
    assert c["swill.prefix"] == "/test"
    assert c["sql"] == {
        "databases": {
            "login": "postgres://",
        },
        "charset": "utf-8",
    }
