import traceback

from surrealdb_rpc.client.websocket import SurrealDBWebsocketClient
from surrealdb_rpc.data_model import Thing


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
        response_select = connection.select("example:123")
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

        response_select = connection.select(["example:123", "example:456"])
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

    def test_object_ids(self, connection: SurrealDBWebsocketClient):
        expected = Thing("test", {"foo": "bar"})
        actual = connection.create("test", {"id": {"foo": "bar"}})["id"]
        assert actual == expected, f"{expected.__msgpack__()} != {actual.__msgpack__()}"

        expected = Thing("test", {"foo": {"bar": "baz"}})
        actual = connection.create("test", {"id": {"foo": {"bar": "baz"}}})["id"]
        assert actual == expected, f"{expected.__msgpack__()} != {actual.__msgpack__()}"

    def test_list_ids(self, connection: SurrealDBWebsocketClient):
        expected = Thing("test", ["foo", "bar"])
        actual = connection.create("test", {"id": ["foo", "bar"]})["id"]
        assert expected == actual, f"{expected.__msgpack__()} != {actual.__msgpack__()}"

        expected = Thing("test", ["foo", {"bar": "baz"}])
        actual = connection.create("test", {"id": ["foo", {"bar": "baz"}]})["id"]
        assert expected == actual, f"{expected.__msgpack__()} != {actual.__msgpack__()}"
