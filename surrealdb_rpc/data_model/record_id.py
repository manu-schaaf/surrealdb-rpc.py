import json

from ulid import encode_random, ulid
from uuid_extensions import uuid7str

from surrealdb_rpc.data_model.string import EscapedString, String
from surrealdb_rpc.data_model.surql import (
    dict_to_surql_str,
    list_to_surql_str,
)
from surrealdb_rpc.data_model.table import Table
from surrealdb_rpc.serialization.abc import JSONSerializable, MsgpackSerializable


class InvalidRecordIdType(ValueError):
    def __init__(self, invalid: type):
        super().__init__(
            "Valid record ID types are: str, int, list | tuple, and dict."
            f" Got: {invalid.__name__}"
        )


class RecordId[T](JSONSerializable, MsgpackSerializable):
    def __init__(self, record_id: T):
        self.value: T = (
            record_id.value if isinstance(record_id, RecordId) else record_id
        )

    @classmethod
    def from_str(cls, string: str, escaped: bool = False) -> "TextRecordId":
        """
        Create a RecordId from a string.
        If `escaped` is set true, any angle-escapes are removed if present.
        """
        if (
            escaped
            and string.startswith("⟨")
            and string.endswith("⟩")
            and not string.endswith("\⟩")
        ):
            string = string[1:-1]
        return cls.new(string)

    @classmethod
    def new(
        cls,
        record_id: T,
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
            >>> RecordId.new("id")
            TextRecordId(id)
            >>> RecordId.new(123)
            NumericRecordId(123)
            >>> RecordId.new("123")
            TextRecordId(123)
            >>> RecordId.new(["hello", "world"])
            ArrayRecordId(['hello', 'world'])
            >>> RecordId.new({'key': 'value'})
            ObjectRecordId({'key': 'value'})

        Raises:
            InvalidRecordId: If the `record_id` type is not supported.
        """
        match record_id:
            case s if isinstance(s, str):
                return TextRecordId(s)
            case i if isinstance(i, int):
                return NumericRecordId(i)
            case ll if isinstance(ll, (list, tuple)):
                return ArrayRecordId(ll)
            case dd if isinstance(dd, dict):
                return ObjectRecordId(dd)
            case _:
                raise InvalidRecordIdType(type(record_id))

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

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value})"

    def __json__(self):
        return json.dumps(self.value)

    def __msgpack__(self) -> str:
        return str(self.id)

    def __eq__(self, other) -> bool:
        return isinstance(other, RecordId) and self.value == other.value


class TextRecordId(RecordId[str]):
    def __msgpack__(self) -> str:
        if self.value.isnumeric():
            return EscapedString.angle(self.value)
        return String.auto_escape(self.value)


class NumericRecordId(RecordId[int]):
    pass


class ObjectRecordId(RecordId[dict]):
    def __msgpack__(self) -> str:
        return dict_to_surql_str(self.value)


class ArrayRecordId(RecordId[list]):
    def __msgpack__(self) -> str:
        return list_to_surql_str(self.value)
