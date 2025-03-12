import json
from asyncio import InvalidStateError
from typing import Any, Literal, Optional

import msgpack
from websockets import WebSocketException
from websockets.protocol import State
from websockets.sync.client import ClientConnection, connect
from websockets.typing import Subprotocol

from surrealdb_rpc.serialization.json import SurrealJSONEncoder
from surrealdb_rpc.serialization.msgpack import (
    msgpack_decode,
    msgpack_encode,
)


class WebsocketSubProtocol:
    def encode(self, data: Any) -> str | bytes: ...

    def decode(self, data: bytes) -> Any: ...


class JSONSubProtocol(WebsocketSubProtocol):
    def encode(self, data: Any) -> bytes:
        data = json.dumps(data, cls=SurrealJSONEncoder, ensure_ascii=False)
        return data.encode("utf-8")

    def decode(self, data: bytes) -> Any:
        return json.loads(data)

    @property
    def protocol(self) -> Subprotocol:
        return Subprotocol("json")


class MsgPackSubProtocol(WebsocketSubProtocol):
    def encode(self, data: Any) -> bytes:
        return msgpack.packb(msgpack_encode(data), default=msgpack_encode)  # type: ignore

    def decode(self, data: bytes) -> Any:
        return msgpack.unpackb(data, ext_hook=msgpack_decode)

    @property
    def protocol(self) -> Subprotocol:
        return Subprotocol("msgpack")


class InvalidResponseError(WebSocketException):
    pass


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
            case data if isinstance(message, bytes):
                self.ws.send(data, text=False)
            case string if isinstance(message, str):
                self.ws.send(string)
            case mapping if isinstance(message, dict):
                self._send(self.sub_protocol.encode(mapping))
            case typ:
                raise TypeError(
                    f"Invalid message type: {typ}",
                    "Message must be a string, bytes or dictionary.",
                )

    def _recv(self) -> Any:
        return self.sub_protocol.decode(self.ws.recv(decode=False))  # type: ignore
