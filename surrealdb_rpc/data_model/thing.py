from typing import Any, Self

from surrealdb_rpc.data_model.record_id import RecordId
from surrealdb_rpc.data_model.table import Table
from surrealdb_rpc.serialization.abc import JSONSerializable, MsgpackSerializable


class ThingStringParseError(ValueError):
    pass


class InvalidThingString(ThingStringParseError):
    def __init__(self, string: str):
        super().__init__(
            f"Thing strings must be composed of a table name and a record ID separated by a colon."
            f" Got: {string}"
        )


class CannotCreateThingFromObj(ValueError):
    def __init__(self, obj: Any):
        super().__init__(
            f"Cannot convert object of type {type(obj).__name__} into a Thing: {str(obj)}"
        )


class Thing[T](JSONSerializable, MsgpackSerializable):
    __reference_class__: type[T]

    def __init__(self, table: Table, id: RecordId):
        self.table: Table = table if isinstance(table, Table) else Table(table)
        self.record_id: RecordId = id if isinstance(id, RecordId) else RecordId(id)

    @classmethod
    def from_str(cls, string: str, escaped: bool = False) -> Self:
        """
        Create a Thing from a string.
        If `escaped` is set true, any angle-escapes are removed if present.
        """
        if ":" not in string:
            raise InvalidThingString(string)

        table, record_id = string.split(":", 1)

        return cls(Table(table), RecordId.new(record_id))

    @classmethod
    def from_raw(cls, string: str) -> Self:
        """
        Create a raw Thing from a string.
        """
        if ":" not in string:
            raise InvalidThingString(string)

        if string.startswith("⟨"):
            end = 0
            while end := string.find("⟩", end + 1):
                if end == -1:
                    raise ThingStringParseError(
                        f"Unbalanced angle brackets in table name for escaped Thing string: {string}"
                    )
                if string[end - 1] != "\\":
                    break

            if string[end + 1] != ":":
                raise ThingStringParseError(
                    f"Expected a colon after the escaped table name:\n  {string}\n  {' ' * end}^"
                )

            table = string[1:end]
            record_id = string[end + 2 :]
        else:
            table, record_id = string.split(":", 1)

        return cls(Table(table), RecordId.from_raw(record_id))

    @classmethod
    def from_obj(cls, obj: Any) -> Self | Table:
        """
        Try to create a new Thing from the given object.

        - If `obj` is a Thing, a copy of it is returned.
        - If `obj` has a `__thing__` method, it is called and the result is returned.
        - If `obj` is a string, it is parsed into a Thing if it contains a colon or a Table otherwise.

        Raises:
            CannotCastIntoThing: If `obj` is not a Thing, a string, or an object with a `__thing__` method.
        """
        match obj:
            case thing if isinstance(obj, Thing):
                return type(thing)(*thing)
            case thingifyable if hasattr(obj, "__thing__") and callable(obj.__thing__):
                return thingifyable.__thing__()
            case string if isinstance(obj, str):
                return cls.from_str(string) if ":" in string else Table(string)
            case _:
                raise CannotCreateThingFromObj(obj)

    def change_table(self, table: str | Table):
        """
        Change the table name of this object.

        Escapes the new table name if necessary.
        """
        self.table = table if isinstance(table, Table) else Table(table)

    def __iter__(self):
        """
        Return an iterator over the components of this object.
        Used to unpack Thing-subclasses into their components.
        """
        yield self.table
        yield self.record_id

    def __repr__(self):
        return f"{type(self).__name__}({self.table}:{self.record_id})"

    def __eq__(self, other):
        return (
            isinstance(other, Thing)
            and self.table == other.table
            and self.record_id == other.record_id
        )

    def __json__(self) -> str:
        """Return a JSON-serializable representation of this object."""
        return f"{self.table.__json__()}:{self.record_id.__json__()}"

    def __msgpack__(self) -> str:
        """Return a msgpack-serializable representation of this object."""
        return f"{self.table.__msgpack__()}:{self.record_id.__msgpack__()}"
