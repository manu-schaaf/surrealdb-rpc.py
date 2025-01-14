from typing import Self


class SurrealDBError(Exception):
    @classmethod
    def from_message(cls, message: str) -> Self:
        match message:
            case "Parse error":
                return ParseError(message)
            case "Invalid request":
                return InvalidRequest(message)
            case "Method not found":
                return MethodNotFound(message)
            case "Method not allowed":
                return MethodNotAllowed(message)
            case "Invalid params":
                return InvalidParams(message)
            case "Live Query was made, but is not supported":
                return LqNotSuported(message)
            case (
                "RT is enabled for the session, but LQ is not supported by the context"
            ):
                return BadLQConfig(message)
            case "A GraphQL request was made, but GraphQL is not supported by the context":
                return BadGQLConfig(message)
            case msg if msg.startswith("There was a problem with the database:"):
                _, internal = msg.split(": ", 1)
                return InternalError(internal.strip())
            case msg if msg.startswith("Error:"):
                _, thrown = msg.split(": ", 1)
                return Thrown(thrown.strip())
            case _:
                return cls(message)


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
