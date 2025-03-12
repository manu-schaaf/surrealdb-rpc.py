from typing import Self

from surrealdb_rpc.data_model.string import String
from surrealdb_rpc.serialization.abc import JSONSerializable, SurrealQLSerializable


class InvalidTableName(ValueError):
    pass


class Table(SurrealQLSerializable, JSONSerializable):
    def __init__(self, table: "str | Table"):
        self.name = table.name if isinstance(table, Table) else table

    @classmethod
    def parse(cls, string: str) -> Self:
        if (
            string.startswith("⟨")
            and string.endswith("⟩")
            and not string.endswith(r"\⟩")
        ):
            string = string[1:-1]
        return cls(string)

    def __repr__(self):
        return f"{type(self).__name__}({self.name})"

    def __eq__(self, other):
        return isinstance(other, Table) and self.name == other.name

    def __json__(self):
        return self.name

    def __surql__(self) -> str:
        return String.auto_escape(self.name)
