import json
from asyncio import InvalidStateError
from typing import Any, Literal, Optional, Self

import msgpack
from requests.auth import _basic_auth_str
from websockets import WebSocketException
from websockets.protocol import State
from websockets.sync.client import ClientConnection, connect
from websockets.typing import Subprotocol

from surrealdb_rpc.protocol import (
    Thing,
    WebSocketJSONEncoder,
    msgpack_decode,
    msgpack_encode,
)


class WebsocketSubProtocol:
    def encode(self, data: Any) -> str | bytes: ...

    def decode(self, data: bytes) -> Any: ...


class JSONSubProtocol(WebsocketSubProtocol):
    def encode(self, data: Any) -> bytes:
        # if isinstance(data, dict):

        data = json.dumps(data, cls=WebSocketJSONEncoder, ensure_ascii=False)
        return data.encode("utf-8")

    def decode(self, data: bytes) -> Any:
        return json.loads(data)

    @property
    def protocol(self) -> Subprotocol:
        return "json"


class MsgPackSubProtocol(WebsocketSubProtocol):
    def encode(self, data: Any) -> bytes:
        return msgpack.packb(data, default=msgpack_encode)

    def decode(self, data: bytes) -> Any:
        return msgpack.unpackb(data, ext_hook=msgpack_decode)

    @property
    def protocol(self) -> Subprotocol:
        return "msgpack"


class WebsocketClient:
    def __init__(
        self, uri, sub_protocol: Literal["json", "msgpack"] = "msgpack", **kwargs
    ):
        self.uri = uri
        self.kwargs = kwargs
        self.__ws: Optional[ClientConnection] = None

        match sub_protocol:
            case "json":
                self.sub_protocol = JSONSubProtocol()
            case "msgpack":
                self.sub_protocol = MsgPackSubProtocol()
            case _:
                raise ValueError(f"Invalid sub-protocol: {sub_protocol}")

    @property
    def ws(self) -> ClientConnection:
        if not self.__ws:
            raise ValueError("Websocket is not connected")
        return self.__ws

    @property
    def state(self) -> State:
        return self.__ws.state if self.__ws else State.CLOSED  # type: ignore

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        match self.state:
            case State.CLOSED:
                return
            case State.OPEN:
                self.ws.close()
            case _:
                raise InvalidStateError(
                    f"Invalid state: Cannot close websocket that is {self.state}"
                )

    def connect(self) -> None:
        match self.state:
            case State.CLOSED:
                self.__ws = connect(
                    self.uri,
                    subprotocols=[self.sub_protocol.protocol],
                    **self.kwargs,
                )
            case State.OPEN:
                return
            case _:
                raise InvalidStateError(
                    f"Invalid state: Cannot connect websocket that is currently {self.state}"
                )

    def _send(self, message: str | bytes | dict) -> None:
        match message:
            case data if isinstance(data, bytes):
                self.ws.send(message, text=False)
            case string if isinstance(string, str):
                self.ws.send(message)
            case mapping if isinstance(mapping, dict):
                self._send(self.sub_protocol.encode(message))
            case typ:
                raise TypeError(
                    f"Invalid message type: {typ}",
                    "Message must be a string, bytes or dictionary.",
                )

    def _recv(self) -> Any:
        return self.sub_protocol.decode(self.ws.recv(decode=False))


class InvalidResponseError(WebSocketException):
    pass


class SurrealDBError(InvalidResponseError):
    @classmethod
    def with_code(cls, code: int, message: str) -> Self:
        return cls(f"SurrealDB Error ({code}): {message}")


class SurrealDBQueryResult(dict):
    @property
    def result(self) -> list[dict]:
        return self["result"]

    @property
    def status(self):
        return self.get("status")

    @property
    def ok(self):
        return self.status == "OK"

    @property
    def time(self):
        return self.get("time")


class SurrealDBClient(WebsocketClient):
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
                case {"code": code, "message": message}:
                    raise SurrealDBError.with_code(code, message)
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
        """Receive a list of results from the websocket connection
        Used for `query`.

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
        """Define a variable on the current connection"""
        self.send("let", [name, value])
        self._recv()
        self.variables.add(name)
        return

    def unset(self, name: str):
        """Remove a variable from the current connection"""
        self.send("unset", [name])
        self._recv()
        self.variables.remove(name)
        return

    def unset_all(self):
        """Remove all variables from the current connection"""
        for variable in self.variables:
            self.unset(variable)
            self.variables.remove(variable)

    def query(self, sql: str, **vars) -> list[SurrealDBQueryResult]:
        params = [sql] if not vars else [sql, vars]
        self.send("query", params)
        return self.recv_query()

    def query_one(self, sql: str, **vars) -> SurrealDBQueryResult:
        results = self.query(sql, **vars)
        if len(results) > 1:
            raise ValueError("Query returned more than one result")
        return results[0]

    def select(self, thing) -> dict | list[dict]:
        """Select either all records in a table or a single record"""
        thing = Thing.new(thing)

        self.send("select", [thing])
        return self.recv()

    def create(
        self,
        thing: str,
        data: dict | None = None,
        **kwargs,
    ) -> dict:
        """Create a record with a random or specified ID in a table

        Args:
            thing (str): Thing to create
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
        thing: str,
        data: dict | list[dict] | None = None,
    ) -> dict | list[dict]:
        """Insert one or multiple records in a table"""
        thing = Thing.new(thing)
        data = data if data is not None else {}
        data = data if isinstance(data, list) else [data]

        self.send("insert", [thing, data])
        return self.recv()

    def insert_relation(
        self,
        table: str | None = None,
        data: dict | None = None,
        **kwargs,
    ) -> dict | list[dict]:
        """Insert a new relation record into a specified table or infer the table from the data"""
        data = data | kwargs if data else kwargs

        self.send("insert_relation", [table, data])
        return self.recv()

    def update(
        self,
        thing: str | list[str],
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
        thing: str | list[str],
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
        record_in: str | list[str],
        relation: str,
        record_out: str | list[str],
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
        thing: str | list[str],
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
        thing: str,
        patches: list[dict],
        diff: bool = False,
    ) -> dict | list[dict]:
        """Patch either all records in a table or a single record with specified patches"""
        thing = Thing.new(thing)

        self.send("patch", [thing, patches, diff])
        return self.recv()

    def delete(
        self,
        thing: str,
    ) -> dict | list[dict]:
        """Delete either all records in a table or a single record"""
        thing = Thing.new(thing)

        self.send("delete", [thing])
        return self.recv()
