from requests.auth import _basic_auth_str

from surrealdb_rpc.client.interface import SurrealDBError, SurrealDBQueryResult
from surrealdb_rpc.client.websocket import InvalidResponseError, WebsocketClient
from surrealdb_rpc.data_model import (
    SingleOrListOfRecordIds,
    SingleRecordId,
    SingleTable,
    Thing,
)


class SurrealDBWebsocketClient(WebsocketClient):
    def __init__(
        self,
        host: str,
        ns: str,
        db: str,
        user: str | None = None,
        password: str | None = None,
        port: int = 8000,
        **kwargs,
    ):
        additional_headers = kwargs.pop("additional_headers", {})
        if "Authorization" not in additional_headers:
            if user and password:
                additional_headers["Authorization"] = _basic_auth_str(user, password)

        self.namespace = ns
        self.database = db

        super().__init__(
            f"ws://{host}:{port}/rpc",
            additional_headers=additional_headers,
            **kwargs,
        )

        self.message_id_counter = 0
        self.variables = set()

    def next_message_id(self) -> int:
        self.message_id_counter += 1
        return self.message_id_counter

    def connect(self) -> None:
        super().connect()
        self.use(self.namespace, self.database)
        return

    def send(self, method, params: list) -> int:
        message_id = self.next_message_id()
        self._send(
            {
                "id": message_id,
                "method": method,
                "params": params,
            }
        )
        return message_id

    def _recv(self):
        response: dict = super()._recv()

        if error := response.get("error"):
            match error:
                # BUG: The returned code is always -32000
                case {
                    # "code": code,
                    "message": message
                }:
                    raise SurrealDBError(message)
                case _:
                    raise SurrealDBError(error)
        return response

    def recv(self) -> dict | list[dict]:
        response = self._recv()

        result: dict | list[dict] | None = response.get("result")
        if result is None:
            raise InvalidResponseError(result)

        return result

    def recv_one(self) -> dict:
        """Receive a single result dictionary from the websocket connection

        Returns:
            SurrealDBResult: A single result dictionary
        """
        result = self.recv()

        if not isinstance(result, dict):
            raise InvalidResponseError(result)

        return result

    def recv_query(self) -> list[SurrealDBQueryResult]:
        """
        Receive a list of results from the websocket connection.
        Used internally for the `query` method.

        Raises:
            InvalidResponseError: If the response is not a list of results

        Returns:
            list[SurrealDBResult]: A list of results
        """
        result = self.recv()

        if not isinstance(result, list):
            raise InvalidResponseError(result)

        return [SurrealDBQueryResult(r) for r in result]

    def use(self, ns: str, db: str) -> None:
        self.send("use", [ns, db])
        self._recv()
        return

    def let(self, name: str, value: str):
        """Define a variable on the current connection."""
        self.send("let", [name, value])
        self._recv()
        self.variables.add(name)
        return

    def unset(self, name: str):
        """Remove a variable from the current connection."""
        self.send("unset", [name])
        self._recv()
        self.variables.remove(name)
        return

    def unset_all(self):
        """Remove all variables from the current connection."""
        for variable in self.variables:
            self.unset(variable)
            self.variables.remove(variable)

    def query(self, sql: str, **vars) -> list[SurrealDBQueryResult]:
        """
        Execute a custom query with optional variables.

        Note:
            Returns a **list of results**, one for each Statement in the query.
        """
        params = [sql] if not vars else [sql, vars]
        self.send("query", params)
        return self.recv_query()

    def query_one(self, sql: str, **vars) -> SurrealDBQueryResult:
        """Conenience method to execute a custom query, returning a single result.

        Note:
            If the query returns more than one result, the last result is returned.
        """
        *_, result = self.query(sql, **vars)
        return result

    def select(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
    ) -> dict | list[dict]:
        """Select either all records in a table or a single record."""
        thing = (
            [Thing.new(el) for el in thing]
            if isinstance(thing, list)
            else Thing.new(thing)
        )

        self.send("select", [thing])
        return self.recv()

    def create(
        self,
        thing: SingleTable | SingleRecordId,
        data: dict | None = None,
        **kwargs,
    ) -> dict:
        """Create a record with a random or specified ID in a table.

        Args:
            thing: The table or record ID to create
            data (optional): Data (key-value) to set on the thing. Defaults to None. Can be set as kwargs or passed as a single dictionary.

        Returns:
            dict: The created record
        """
        thing = Thing.new(thing)
        data = data | kwargs if data else kwargs

        self.send("create", [thing, data])
        return self.recv_one()

    def insert(
        self,
        table: SingleTable,
        data: dict | list[dict] | None = None,
    ) -> dict | list[dict]:
        """Insert one or multiple records in a table."""
        data = data if data is not None else {}
        data = data if isinstance(data, list) else [data]

        self.send("insert", [table, data])
        return self.recv()

    def insert_relation(
        self,
        table: SingleTable | None = None,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Insert a new relation record into a specified table or infer the table from the data"""
        data = data | kwargs if data else kwargs

        self.send("insert_relation", [table, data])
        return self.recv()

    def update(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Modify either all records in a table or a single record with specified data if the record already exists"""
        thing = (
            [Thing.new(t) for t in thing]
            if isinstance(thing, list)
            else Thing.new(thing)
        )
        data = data | kwargs if data else kwargs

        self.send("update", [thing, data])
        return self.recv()

    def upsert(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Replace either all records in a table or a single record with specified data"""
        thing = (
            [Thing.new(t) for t in thing]
            if isinstance(thing, list)
            else Thing.new(thing)
        )
        data = data | kwargs if data else kwargs

        self.send("upsert", [thing, data])
        return self.recv()

    def relate(
        self,
        record_in: SingleTable | SingleOrListOfRecordIds,
        relation: str,
        record_out: SingleTable | SingleOrListOfRecordIds,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Create graph relationships between created records"""
        record_in = (
            [Thing.new(thing) for thing in record_in]
            if isinstance(record_in, list)
            else Thing.new(record_in)
        )
        record_out = (
            [Thing.new(thing) for thing in record_out]
            if isinstance(record_out, list)
            else Thing.new(record_out)
        )
        data = data | kwargs if data else kwargs

        self.send("relate", [record_in, relation, record_out, data])
        return self.recv()

    def merge(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Merge specified data into either all records in a table or a single record"""
        thing = (
            [Thing.new(t) for t in thing]
            if isinstance(thing, list)
            else Thing.new(thing)
        )
        data = data | kwargs if data else kwargs

        self.send("merge", [thing, data])
        return self.recv()

    def patch(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
        patches: list[dict],
        diff: bool = False,
    ) -> dict | list[dict]:
        """Patch either all records in a table or a single record with specified patches"""
        thing = Thing.new(thing)

        self.send("patch", [thing, patches, diff])
        return self.recv()

    def delete(
        self,
        thing: SingleTable | SingleOrListOfRecordIds,
    ) -> dict | list[dict]:
        """Delete either all records in a table or a single record"""
        thing = Thing.new(thing)

        self.send("delete", [thing])
        return self.recv()
