from surrealdb_rpc.data_model.string import String
from surrealdb_rpc.serialization.abc import JSONSerializable, MsgpackSerializable


class InvalidTableName(ValueError):
    pass


class Table(MsgpackSerializable, JSONSerializable):
    def __init__(self, table: "str | Table"):
        self.name = table.name if isinstance(table, Table) else table

    def __repr__(self):
        return f"{type(self).__name__}({self.name})"

    def __eq__(self, other):
        return isinstance(other, Table) and self.name == other.name

    def __json__(self):
        return self.name

    def __msgpack__(self) -> str:
        return String.auto_escape(self.name)
