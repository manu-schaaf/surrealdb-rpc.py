from typing import Self


class SurrealDBError(Exception):
    @classmethod
    def from_message(cls, message: str) -> Self:
        match message:
            case "Parse error":
                return ParseError(message)  # type: ignore
            case "Invalid request":
                return InvalidRequest(message)  # type: ignore
            case "Method not found":
                return MethodNotFound(message)  # type: ignore
            case "Method not allowed":
                return MethodNotAllowed(message)  # type: ignore
            case "Invalid params":
                return InvalidParams(message)  # type: ignore
            case "Live Query was made, but is not supported":
                return LqNotSuported(message)  # type: ignore
            case (
                "RT is enabled for the session, but LQ is not supported by the context"
            ):
                return BadLQConfig(message)  # type: ignore
            case "A GraphQL request was made, but GraphQL is not supported by the context":
                return BadGQLConfig(message)  # type: ignore
            case msg if msg.startswith("There was a problem with the database:"):
                _, internal = msg.split(": ", 1)
                return InternalError(internal.strip())  # type: ignore
            case msg if msg.startswith("Error:"):
                _, thrown = msg.split(": ", 1)
                return Thrown(thrown.strip())  # type: ignore
            case _:
                return cls(message)


class EmptyResponse(SurrealDBError):
    pass


class InvalidResultType(SurrealDBError):
    result: object

    def __init__(self, expected: type, result: object):
        super().__init__(
            f"Expected result of type {expected.__name__} but got: {type(result).__name__}"
        )


class ParseError(SurrealDBError):
    pass


class InvalidRequest(SurrealDBError):
    pass


class MethodNotFound(SurrealDBError):
    pass


class MethodNotAllowed(SurrealDBError):
    pass


class InvalidParams(SurrealDBError):
    pass


class LqNotSuported(SurrealDBError):
    pass


class BadLQConfig(SurrealDBError):
    pass


class BadGQLConfig(SurrealDBError):
    pass


class InternalError(SurrealDBError):
    pass


class Thrown(SurrealDBError):
    pass


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
