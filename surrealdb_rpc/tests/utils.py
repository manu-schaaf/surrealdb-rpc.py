from surrealdb_rpc.client.websocket import SurrealDBClient
from surrealdb_rpc.data_model import RecordId


def _test_create_select_query(port: int, user: str, password: str):
    with SurrealDBClient(
        host="localhost",
        port=18000,
        ns="test",
        db="test",
        user="root",
        password="root",
    ) as connection:
        example_id = RecordId.new("example", "123")
        response_create = connection.create(
            # specify RecordId as string (will create a TextRecordId)
            "example:123",
            # specify fields as kwargs
            text="Some value",
            # lists for arrays
            array=[1, 2, 3],
            # regular dicts for objects
            object={"key": "value"},
            # RecordId object with automatic ID escaping
            reference=RecordId.new("other", {"foo": {"bar": "baz"}}),
        )
        # SurrealDBClient.create returns the created record
        assert response_create["id"] == example_id

        # Fetch a single record by ID
        response_select = connection.select("example:123")
        assert response_select == response_create

        # Run a SurrealQL query
        response_query = connection.query(
            'SELECT * FROM example WHERE text = "Some value"'
        )
        # Returns a result for each statement in the query
        assert len(response_query) == 1
        first_response = response_query[0]
        # Retrieve actual result with the "result" key
        first_response_result = first_response["result"]
        # `SELECT` returns a list of records
        assert len(first_response_result) == 1
        assert first_response_result[0] == response_create
