from typing import Required, TypedDict

from requests.auth import _basic_auth_str

from surrealdb_rpc.client.interface import (
    EmptyResponse,
    InvalidResultType,
    SurrealDBError,
    SurrealDBQueryResult,
)
from surrealdb_rpc.client.websocket import WebsocketClient
from surrealdb_rpc.data_model import (
    OneOrManyThings,
    SingleTable,
    SingleThing,
    Table,
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
                    raise SurrealDBError.from_message(message)
                case _:
                    raise SurrealDBError(error)
        return response

    def recv(self, empty_response_is_error=True) -> dict | list[dict] | None:
        response = self._recv()

        result: dict | list[dict] | None = response.get("result")
        if empty_response_is_error and result is None:
            raise EmptyResponse(result)

        return result

    def recv_one(self) -> dict:
        """Receive a single result dictionary from the websocket connection

        Raises:
            InvalidResultType: If the result is not a single dictionary.

        Returns:
            SurrealDBResult: A single result dictionary
        """
        result = self.recv()

        if not isinstance(result, dict):
            raise InvalidResultType(dict, result)

        return result

    def recv_query(self) -> list[SurrealDBQueryResult]:
        """Receive a list of query results from the websocket connection.
        Used internally for the `query` method.

        Raises:
            InvalidResultType: If the result is not a list.

        Returns:
            list[SurrealDBResult]: A list of results
        """
        result = self.recv()

        if not isinstance(result, list):
            raise InvalidResultType(list, result)

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
        """Convenience method to execute a custom query, returning a single result.

        Note:
            If the query returns more than one result, the last result is returned.
        """
        *_, result = self.query(sql, **vars)
        return result

    def select(
        self,
        thing: SingleTable | OneOrManyThings,
    ) -> None | dict | list[dict]:
        """Select either all records in a table or a single record.

        Returns:
            None: The thing was not found
            dict | list[dict]: The selected record(s)
        """
        if isinstance(thing, list):
            self.send("select", [[Thing.from_obj(el) for el in thing]])
        else:
            self.send("select", [Thing.from_obj_maybe_table(thing)])

        return self.recv(empty_response_is_error=False)

    def create(
        self,
        thing: SingleTable | SingleThing,
        data: dict | None = None,
        **kwargs,
    ) -> dict:
        """Create a single record with a random or specified ID in the given table.

        Args:
            thing: The table or record ID to create.
            data (optional): Data (key-value) to set on the thing. Defaults to None. Can be set as kwargs or passed as a single dictionary.

        Returns:
            dict: The created record.
        """
        thing_or_table: Thing | Table = Thing.from_obj_maybe_table(thing)
        data = data | kwargs if data else kwargs

        self.send("create", [thing_or_table, data])
        return self.recv_one()

    def insert(
        self,
        table: SingleTable,
        data: dict | list[dict] | None = None,
    ) -> list[dict]:
        """Insert one or multiple records in a table."""
        data = data if data is not None else {}
        data = data if isinstance(data, list) else [data]

        self.send("insert", [table, data])
        return self.recv()  # type: ignore

    InsertRelation = TypedDict(
        "InsertRelation",
        {
            "in": Required[Thing],
            "out": Required[Thing],
        },
        total=False,
    )

    def insert_relation(
        self,
        table: SingleTable,
        data: InsertRelation | list[InsertRelation],
        **kwargs,
    ) -> dict | list[dict]:
        """Insert a new relation record into a specified table or infer the table from the data.

        Args:
            table (SingleTable): The table to insert the relation into.
            data (InsertRelation | list[InsertRelation]): The relation data to insert.
            **kwargs: Additional keyword arguments to be included in the relation data.

        Returns:
            dict | list[dict]: The inserted relation record(s).

        Raises:
            ValueError: If the data is not a dictionary or a list of dictionaries.

        Note:
            If given, the `kwargs` will be added to **every** relation record.
        """
        if isinstance(data, list):
            data = [d | kwargs for d in data]  # type: ignore
        elif isinstance(data, dict):
            data |= kwargs  # type: ignore
        else:
            raise ValueError("Data must be a dictionary or a list of dictionaries")

        self.send("insert_relation", [Table(table), data])
        return self.recv()  # type: ignore

    def update(
        self,
        thing: SingleTable | OneOrManyThings,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Modify either all records in a table or a single record with specified data if the record already exists"""
        data = data | kwargs if data else kwargs

        if isinstance(thing, list):
            self.send("update", [[Thing.from_obj(t) for t in thing], data])
        else:
            self.send("update", [Thing.from_obj_maybe_table(thing), data])

        return self.recv()  # type: ignore

    def upsert(
        self,
        thing: SingleTable | OneOrManyThings,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Replace either all records in a table or a single record with specified data"""
        data = data | kwargs if data else kwargs
        if isinstance(thing, list):
            self.send("upsert", [[Thing.from_obj(t) for t in thing], data])
        else:
            self.send("upsert", [Thing.from_obj_maybe_table(thing), data])

        return self.recv()  # type: ignore

    def relate(
        self,
        record_in: SingleTable | OneOrManyThings,
        relation: str | Table,
        record_out: SingleTable | OneOrManyThings,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Create graph relationships between created records"""
        things_in: Table | Thing | list[Thing] = (
            [Thing.from_obj(thing) for thing in record_in]
            if isinstance(record_in, list)
            else Thing.from_obj_maybe_table(record_in)
        )
        things_out: Table | Thing | list[Thing] = (
            [Thing.from_obj(thing) for thing in record_out]
            if isinstance(record_out, list)
            else Thing.from_obj_maybe_table(record_out)
        )
        data = data | kwargs if data else kwargs

        self.send("relate", [things_in, Table(relation), things_out, data])
        return self.recv()  # type: ignore

    def merge(
        self,
        thing: SingleTable | OneOrManyThings,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Merge specified data into either all records in a table or a single record"""
        data = data | kwargs if data else kwargs
        if isinstance(thing, list):
            thing = [Thing.from_obj(t) for t in thing]
        else:
            thing = Thing.from_obj_maybe_table(thing)

        self.send("merge", [thing, data])
        return self.recv()  # type: ignore

    def patch(
        self,
        thing: SingleTable | OneOrManyThings,
        patches: list[dict],
        diff: bool = False,
    ) -> dict | list[dict]:
        """Patch either all records in a table or a single record with specified patches"""
        if isinstance(thing, list):
            self.send("patch", [[Thing.from_obj(t) for t in thing], patches, diff])
        else:
            self.send("patch", [Thing.from_obj_maybe_table(thing), patches, diff])

        return self.recv()  # type: ignore

    def delete(
        self,
        thing: SingleTable | OneOrManyThings,
    ) -> dict | list[dict]:
        """Delete either all records in a table or a single record"""
        if isinstance(thing, list):
            self.send("delete", [Thing.from_obj(t) for t in thing])
        else:
            self.send("delete", [Thing.from_obj_maybe_table(thing)])

        return self.recv()  # type: ignore
