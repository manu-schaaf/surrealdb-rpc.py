import traceback

from surrealdb_rpc.client.interface import InternalError as SurrealDBInternalError
from surrealdb_rpc.client.websocket import SurrealDBWebsocketClient
from surrealdb_rpc.data_model import Thing
from surrealdb_rpc.data_model.table import Table
from surrealdb_rpc.data_model.thing import CannotCreateThingFromObj


class Queries:
    def test_base_queries(self, connection: SurrealDBWebsocketClient):
        example_id = Thing("example", 123)
        response_create = connection.create(
            # specify Thing as string (will create a TextRecordId)
            "example:123",
            # specify fields as kwargs
            text="Some value",
            # lists for arrays
            array=[1, 2, 3],
            # regular dicts for objects
            object={"key": "value"},
            # Thing object with automatic record ID escaping
            reference=Thing("other", {"foo": {"bar": "baz"}}),
        )
        # SurrealDBClient.create returns the created record
        assert response_create["id"] == example_id, (
            f"{response_create['id']} != {example_id}"
        )

        # Fetch a single record by ID
        response_select: list[dict] = connection.select("example:123")  # type: ignore
        assert response_select == response_create

        # Run a SurrealQL query
        response_query = connection.query(
            'SELECT * FROM example WHERE text = "Some value"'
        )
        # Returns a result for each statement in the query
        assert len(response_query) == 1, f"Expected 1 result but got {response_query}"
        first_response = response_query[0]
        # Retrieve actual result with the "result" key
        first_response_result = first_response["result"]
        # `SELECT` returns a list of records
        assert len(first_response_result) == 1, (
            f"Expected 1 record but got {first_response_result}"
        )
        assert first_response_result[0] == response_create, (
            f"{first_response_result[0]} != {response_create}"
        )

        # Use the insert method to insert multiple records at once
        connection.insert(
            "example",
            [
                {
                    "id": 456,  # You can specify an ID as a field ...
                    "text": "Another value",
                    "array": [42],
                    "object": {"foo": "bar"},
                },
                {
                    # ... or omit the ID to generate a random one
                    "text": "...",
                    "array": [1337],
                    "object": {"value": "key"},
                    "reference": None,  # None is mapped to NULL in the database
                },
            ],
        )

        response_select = connection.select(["example:123", "example:456"])  # type: ignore
        assert len(response_select) == 2, (
            f"Expected 2 records but got {response_select}"
        )
        assert response_create in response_select, (
            f"{response_create} not in {response_select}"
        )

    def test_expected_failures(self, connection: SurrealDBWebsocketClient):
        try:
            response_select = connection.select("example:nonexistent")
            assert response_select is None

            response_query = connection.query_one("SELECT * FROM nonexistent")
            assert response_query.result == [], (
                f"Expected empty list but got: {response_query}"
            )
        except AssertionError as e:
            raise e
        except Exception:
            raise AssertionError(
                f"Expected emtpy response but got error:\n{traceback.format_exc()}"
            )

    def test_complex_strings(self, connection: SurrealDBWebsocketClient):
        response, *_ = connection.insert("complex table name", {"id": "foo"})
        assert response["id"] == Thing.parse("complex table name:foo")

        response = connection.create("test", {"id": "foo-bar"})
        assert response["id"] == Thing.parse("test:foo-bar")

        response = connection.create("test:bar-baz")
        assert response["id"] == Thing("test", "bar-baz")

        response = connection.create(
            "test", value="'value'", mapping={"key": "'value'"}
        )
        assert response["value"] == "'value'"
        assert response["mapping"]["key"] == "'value'"

        response = connection.create("test", value="abc-def.xyz")
        assert response["value"] == "abc-def.xyz"

    def test_object_ids(self, connection: SurrealDBWebsocketClient):
        expected = Thing("test", {"foo": "bar"})
        actual = connection.create("test", {"id": {"foo": "bar"}})["id"]
        assert actual == expected, f"{expected.__surql__()} != {actual.__surql__()}"

        expected = Thing("test", {"foo": {"bar": "baz"}})
        actual = connection.create("test", {"id": {"foo": {"bar": "baz"}}})["id"]
        assert actual == expected, f"{expected.__surql__()} != {actual.__surql__()}"

    def test_list_ids(self, connection: SurrealDBWebsocketClient):
        expected = Thing("test", ["foo", "bar"])
        actual = connection.create("test", {"id": ["foo", "bar"]})["id"]
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

        expected = Thing("test", ["foo", {"bar": "baz"}])
        actual = connection.create("test", {"id": ["foo", {"bar": "baz"}]})["id"]
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

    def test_array_of_records(self, connection: SurrealDBWebsocketClient):
        expected = Thing("test", 123)
        actual = connection.create(
            "test",
            {
                "array_of_records": [
                    expected,
                    Thing("test", 234),
                    Thing("test", 345),
                ]
            },
        )["array_of_records"][0]
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

    def test_relations(self, connection: SurrealDBWebsocketClient):
        connection.insert(
            "test",
            [{"id": i} for i in range(10)],
        )
        connection.relate(Thing("test", 0), "single", Thing("test", 1))

        response: list[dict] = connection.query_one(
            "SELECT id AS from, ->single->test as to FROM test WHERE ->single"
        ).result

        actual = response[0]["from"]
        expected = Thing("test", 0)
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

        actual = response[0]["to"]
        expected = [Thing("test", 1)]
        assert expected == actual, (
            f"[{expected[0].__surql__()}] != {actual.__surql__()}"
        )

        connection.relate(
            Thing("test", 1),
            "one_to_many",
            [
                Thing("test", 2),
                Thing("test", 3),
                Thing("test", 4),
            ],
        )
        response = connection.query_one(
            "SELECT count() FROM (SELECT ->one_to_many->test AS r FROM test WHERE ->one_to_many SPLIT r) GROUP ALL;"
        )["result"]
        assert response[0]["count"] == 3, (
            f"Expected 3 relations from one-to-many RELATE, but got {response[0]['count']}"
        )

        connection.relate(
            [
                Thing("test", 0),
                Thing("test", 1),
                Thing("test", 2),
            ],
            "many_to_many",
            [
                Thing("test", 3),
                Thing("test", 4),
                Thing("test", 5),
            ],
        )
        response = connection.query_one(
            "SELECT count() FROM (SELECT ->many_to_many->test AS r FROM test SPLIT r) GROUP ALL;"
        )["result"]
        assert response[0]["count"] == 9, (
            f"Expected 9 relations from cartesian product RELATE, but got {response[0]['count']}"
        )

        connection.insert_relation(
            "insert_one", {"in": Thing("test", 0), "out": Thing("test", 1)}
        )
        response = connection.query_one(
            "SELECT id AS from, ->insert_one->test as to FROM test WHERE ->insert_one"
        )["result"]

        actual = response[0]["from"]
        expected = Thing("test", 0)
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

        actual = response[0]["to"]
        expected = [Thing("test", 1)]
        assert expected == actual, (
            f"[{expected[0].__surql__()}] != {actual.__surql__()}"
        )

        connection.insert_relation(
            "insert_one_kwargs",
            {"in": Thing("test", 0), "out": Thing("test", 1)},
            some="field",
        )
        response = connection.query_one("SELECT some FROM insert_one_kwargs")["result"]
        assert response[0]["some"] == "field", (
            f"Expected 'field' but got {response[0]['field']}"
        )

        connection.insert_relation(
            "insert_many",
            [
                {"in": Thing("test", 0), "out": Thing("test", 1)},
                {"in": Thing("test", 2), "out": Thing("test", 3)},
            ],
            key="value",
        )
        response = connection.query_one(
            "SELECT id AS from, ->insert_many->test as to FROM test WHERE ->insert_many"
        )["result"]

        actual = response[0]["from"]
        expected = Thing("test", 0)
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

        actual = response[0]["to"]
        expected = [Thing("test", 1)]
        assert expected == actual, (
            f"[{expected[0].__surql__()}] != {actual.__surql__()}"
        )

        actual = response[1]["from"]
        expected = Thing("test", 2)
        assert expected == actual, f"{expected.__surql__()} != {actual.__surql__()}"

        actual = response[1]["to"]
        expected = [Thing("test", 3)]
        assert expected == actual, (
            f"[{expected[0].__surql__()}] != {actual.__surql__()}"
        )

        response = connection.select("insert_many")  # type: ignore
        assert len(response) == 2, (
            f"Expected 2 relations in insert_many, but got {len(response)}"
        )

        actual = [r["key"] for r in response]
        expected = ["value" for _ in response]
        assert expected == actual, f"{expected} != {actual}"

    def test_typed_table(self, connection: SurrealDBWebsocketClient):
        connection.query(
            """
            DEFINE table test_typed TYPE NORMAL;
            DEFINE FIELD text ON TABLE test_typed TYPE string;
            """
        )

        try:
            connection.query("CREATE test_typed SET text = None;")
        except Exception as e:
            raise AssertionError from e

        try:
            connection.create("test_typed", text=None)
        except SurrealDBInternalError as e:
            if "Found NONE for field" not in str(e):
                raise AssertionError(
                    "Expected SurrealDB InternalError 'Found NONE for field' "
                    "when creating a record with a None value using the RPC create method "
                    f"but got: '{e}'"
                ) from e
        except Exception as e:
            raise AssertionError from e
        else:
            raise AssertionError(
                "Expected SurrealDB InternalError 'Found NONE for field' "
                "when creating a record with a None value using the RPC create method"
            )

    def test_typed_table_option(self, connection: SurrealDBWebsocketClient):
        connection.query(
            """
            DEFINE table test_typed_option TYPE NORMAL;
            DEFINE FIELD OVERWRITE text ON TABLE test_typed_option TYPE option<string>;
            """
        )

        try:
            connection.query("CREATE test_typed_option SET text = None;")
        except Exception as e:
            raise AssertionError from e

        try:
            connection.create(
                "test_typed_option",
                text=None,
            )
        except Exception as e:
            raise AssertionError from e

    def test_select(self, connection: SurrealDBWebsocketClient):
        response_one = connection.insert("table_one", {"id": 1})
        response_two = connection.insert("table_two", [{"id": 1}, {"id": 2}])

        response_select = connection.select("table_one")
        assert response_select == response_one, response_select

        response_select = connection.select("table_two")
        assert response_select == response_two, response_select

        response_select = connection.select(Table("table_one"))
        assert response_select == response_one, response_select

        response_select = connection.select(Table("table_two"))
        assert response_select == response_two, response_select

        response_select = connection.select(
            ["table_one:1", "table_two:1", "table_two:2"]
        )
        assert len(response_select) == 3, response_select  # type: ignore

        try:
            response_select = connection.select(
                [Table("table_one"), Table("table_two")]  # type: ignore
            )
        except CannotCreateThingFromObj:
            pass
        else:
            raise AssertionError("Expected CannotCreateThingFromObj")

    def test_update(self, connection: SurrealDBWebsocketClient):
        connection.insert("test_update", [{"id": 1}, {"id": 2}, {"id": 3}])

        response: list[dict] = connection.update("test_update", key="value")  # type: ignore
        assert len(response) == 3, response
        assert all(r["key"] == "value" for r in response), response

        response = connection.update("test_update", {"key": "other"})  # type: ignore
        assert len(response) == 3, response
        assert all(r["key"] == "other" for r in response), response

        response = connection.update("test_update", {"other": "value"}, key="value")  # type: ignore
        assert len(response) == 3, response
        assert all(r["key"] == "value" for r in response), response
        assert all(r["other"] == "value" for r in response), response

        connection.update("test_update:2", odd=False)
        connection.update(["test_update:1", "test_update:3"], odd=True)

        response = connection.select("test_update")  # type: ignore
        assert len(response) == 3, response
        assert all("odd" in r for r in response), response
        assert all(
            int(r["id"].record_id.value) % 2 == int(r["odd"]) for r in response
        ), response
