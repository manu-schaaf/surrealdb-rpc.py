import json
import warnings
from abc import ABC, abstractmethod
from typing import Any, Self, TypeGuard

from ulid import encode_random, ulid
from uuid_extensions import uuid7str

from surrealdb_rpc.data_model.ext_types import UUID, DateTime, Decimal, Duration
from surrealdb_rpc.data_model.string import EscapedString, String



class InvalidRecordIdType(ValueError):
    def __init__(self, invalid: type):
        super().__init__(
            "Valid record ID types are: str, int, list | tuple, and dict."
            f" Got: {invalid.__name__}"
        )


type TableNameStr = str
type RecordIdStr = str


def is_record_id_str(string: str) -> TypeGuard[RecordIdStr]:
    """Record ID strings must be composed of a table name and a record ID separated by a colon."""
    match string.split(":", maxsplit=1):
        case [table, id]:
            return table and id
        case _:
            return False


class InvalidRecordId(ValueError):
    pass


class InvalidRecordIdString(InvalidRecordId):
    def __init__(self, string: str):
        super().__init__(
            f"Record ID strings must be composed of a table name and a record ID separated by a colon."
            f" Got: {string}"
        )


def validate_record_id_str(string: str) -> None:
    """Check a record ID string, raise a ValueError if it is invalid.

    Raises:
        InvalidRecordId: If the record ID is invalid.
    """
    if not is_record_id_str(string):
        raise InvalidRecordIdString(string)


class CannotConvertIntoThing(ValueError):
    def __init__(self, obj: Any):
        super().__init__(
            f"Cannot convert object of type {type(obj).__name__} into a Thing: {str(obj)}"
        )


class Thing[R](ABC):
    __reference_class__: type[R]

    @classmethod
    def from_str(
        cls, s: TableNameStr | RecordIdStr, escaped: bool = False
    ) -> "Table | RecordId":
        if ":" in s:
            return RecordId.from_str(s)
        return Table(s)

    @classmethod
    def new(cls, obj: Any) -> "Table | RecordId":
        if isinstance(obj, Thing) and hasattr(obj, "__iter__"):
            return type(obj)(*obj)
        elif hasattr(obj, "__thing__") and callable(obj.__thing__):
            return obj.__thing__()
        elif isinstance(obj, str):
            return cls.from_str(obj)
        else:
            raise CannotConvertIntoThing(obj)

    @abstractmethod
    def __iter__(self):
        """
        Return an iterator over the components of this object.
        Used to unpack Thing-subclasses into their components.
        """
        ...

    @abstractmethod
    def __json__(self) -> str:
        """Return a JSON-serializable representation of this object."""
        ...

    @abstractmethod
    def __pack__(self) -> str:
        """Return a msgpack-serializable representation of this object."""
        ...


class Table(Thing):
    def __init__(self, table: "TableNameStr | Table"):
        if isinstance(table, Table):
            self.table = table.table
        else:
            self.table = String.auto_escape(table)

    def __repr__(self):
        return f"{type(self).__name__}({self.table})"

    def __iter__(self):
        yield self.table

    def __eq__(self, other):
        return self.table == other.table

    def __json__(self):
        return self.table

    def __pack__(self) -> str:
        return self.table

    def change_table(self, table: TableNameStr):
        """
        Change the table name of this object.

        Escapes the new table name if necessary.
        """
        self.table = String.auto_escape(table)


class RecordId[T](Table):
    def __init__(self, table: TableNameStr, id: T):
        super().__init__(table)
        self.id: T = id

    def __repr__(self):
        return f"{type(self).__name__}({self.table}:{str(self.id)})"

    def __iter__(self):
        yield self.table
        yield self.id

    def __eq__(self, other):
        return self.table == other.table and self.id == other.id

    def __json__(self):
        return f"{String.auto_escape(self.table)}:{json.dumps(self.id)}"

    def __pack__(self) -> str:
        return f"{String.auto_escape(self.table)}:{self.__pack_id__()}"

    @abstractmethod
    def __pack_id__(self) -> str:
        return str(self.id)

    @classmethod
    def from_str(cls, string: RecordIdStr, escaped: bool = False) -> "TextRecordId":
        """
        Create a TextRecordId from a string.

        Note:
            Do not use with complex, escaped strings!
        """
        match string.split(":", maxsplit=1):
            case [table, id]:
                return RecordId.new(table, EscapedString(id) if escaped else id)
            case _:
                raise InvalidRecordIdString(string)

    @classmethod
    def new(
        cls,
        table: str | Table,
        id: T,
    ) -> "TextRecordId | NumericRecordId | ObjectRecordId | ArrayRecordId":
        """
        Create a new typed RecordId object. The type is inferred from the `id` argument.

        Note:
            Supported types:
            - `TextRecordId`: `str`
            - `NumericRecordId`: `int`
            - `ArrayRecordId`: `list` | `tuple`
            - `ObjectRecordId`: `dict`

        Examples:
            >>> RecordId.new("table", "id")
            TextRecordId(table:id)
            >>> RecordId.new("table", 123)
            NumericRecordId(table:123)
            >>> RecordId.new("table", "123")
            TextRecordId(table:123)
            >>> RecordId.new("table", ["hello", "world"])
            ArrayRecordId(table:['hello', 'world'])
            >>> RecordId.new('table', {'key': 'value'})
            ObjectRecordId(table:{'key': 'value'})

        Raises:
            InvalidRecordId: If the ID type is not supported.
        """
        match id:
            case s if isinstance(id, str):
                return TextRecordId(table, s)
            case i if isinstance(id, int):
                return NumericRecordId(table, i)
            case ll if isinstance(id, (list, tuple)):
                return ArrayRecordId(table, ll)
            case dd if isinstance(id, dict):
                return ObjectRecordId(table, dd)
            case _:
                raise InvalidRecordId(
                    "Valid record ID types are: str, int, list | tuple, and dict."
                    f" Got: {type(id)}"
                )

    @classmethod
    def rand(cls, table: str | Table) -> "TextRecordId":
        """Generate a 20-character (a-z0-9) record ID."""
        return TextRecordId(table, encode_random(20).lower())

    @classmethod
    def ulid(cls, table: str | Table) -> "TextRecordId":
        """Generate a ULID-based record ID."""
        return TextRecordId(table, ulid().lower())

    @classmethod
    def uuid(cls, table: str | Table, ns: int | None = None) -> "TextRecordId":
        """Generate a UUIDv7-based record ID."""
        return TextRecordId(table, uuid7str(ns))


class TextRecordId(RecordId[str]):
    def __pack_id__(self) -> str:
        if self.id.isnumeric():
            return EscapedString.angle(self.id)
        return String.auto_escape(self.id)


class NumericRecordId(RecordId[int]):
    pass


class ObjectRecordId(RecordId[dict]):
    def __pack_id__(self) -> str:
        return (
            "{"
            + ",".join(
                String.auto_escape(k) + ":" + pack_record_id(v, quote=True)
                for k, v in self.id.items()
            )
            + "}"
        )


class ArrayRecordId(RecordId[list]):
    pass


ExtTypes = None | UUID | Decimal | Duration | DateTime | RecordId
SurrealTypes = None | bool | int | float | str | bytes | list | dict | ExtTypes


def pack_record_id(value: SurrealTypes, quote: bool = False) -> None | str:
    """Convert a field value to a msgpack-serializable string.

    Examples:
        >>> pack_record_id(42)
        '42'
        >>> pack_record_id("table")
        'table'
        >>> pack_record_id(RecordId.new("table", "id"))
        'table:id'
        >>> pack_record_id({"simple_key": "value"})
        '{simple_key:"value"}'
        >>> pack_record_id(["hello", "world"])
        '["hello","world"]'
    """
    match value:
        case s if isinstance(s, str):
            return String.auto_quote(s, True) if quote else String.auto_escape(s)
        case i if isinstance(i, int):
            return str(i)
        case ll if isinstance(ll, (list, tuple)):
            return f"[{','.join(pack_record_id(e, True) for e in ll)}]"
        case dd if isinstance(dd, dict):
            return (
                "{"
                + ",".join(
                    f"{String.auto_escape(k, True)}:{pack_record_id(v, True)}"
                    for k, v in dd.items()
                )
                + "}"
            )
        case rid if isinstance(rid, RecordId):
            return rid.__pack__()
        case _:
            raise NotImplementedError
