from typing import Self


class SurrealDBError(Exception):
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
