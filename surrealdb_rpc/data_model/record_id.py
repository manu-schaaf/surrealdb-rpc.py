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
    def new(
        cls,
        record_id: T,
    ) -> "TextRecordId | NumericRecordId | ObjectRecordId | ArrayRecordId":
        """
        Create a new typed RecordId object. The type is inferred from the `id` argument.

        Note:
            Supported types:
            - `TextRecordId`: `str`
            - `NumericRecordId`: `int` and numeric strings
            - `ArrayRecordId`: `list` | `tuple`
            - `ObjectRecordId`: `dict`

        Examples:
            >>> RecordId.new("id")
            TextRecordId(id)
            >>> RecordId.new(123)
            NumericRecordId(123)
            >>> RecordId.new("123")
            NumericRecordId(123)
            >>> RecordId.new(["hello", "world"])
            ArrayRecordId(['hello', 'world'])
            >>> RecordId.new({'key': 'value'})
            ObjectRecordId({'key': 'value'})

        Raises:
            InvalidRecordId: If the `record_id` type is not supported.
        """
        match record_id:
            case s if isinstance(s, str) and not s.isnumeric():
                return TextRecordId(s)
            case i if isinstance(i, (str, int)):
                return NumericRecordId(i)
            case ll if isinstance(ll, (list, tuple)):
                return ArrayRecordId(ll)
            case dd if isinstance(dd, dict):
                return ObjectRecordId(dd)
            case _:
                raise InvalidRecordIdType(type(record_id))

    @classmethod
    def from_raw(cls, string: str) -> "RawRecordId":
        """
        Create a raw RecordId from a string.
        """
        return RawRecordId(string)

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
        match self.value:
            case s if isinstance(s, str):
                return TextRecordId.__msgpack__(self)
            case i if isinstance(i, int):
                return NumericRecordId.__msgpack__(self)
            case ll if isinstance(ll, (list, tuple)):
                return ArrayRecordId.__msgpack__(self)
            case dd if isinstance(dd, dict):
                return ObjectRecordId.__msgpack__(self)
            case _:
                raise NotImplementedError

    def __eq__(self, other) -> bool:
        return isinstance(other, RecordId) and self.__msgpack__() == other.__msgpack__()


class TextRecordId(RecordId[str]):
    def __msgpack__(self) -> str:
        if self.value.isnumeric():
            return EscapedString.angle(self.value)
        return String.auto_escape(self.value)


class NumericRecordId(RecordId[int]):
    def __msgpack__(self):
        return str(self.value)


class ObjectRecordId(RecordId[dict]):
    def __msgpack__(self) -> str:
        return dict_to_surql_str(self.value)


class ArrayRecordId(RecordId[list]):
    def __msgpack__(self) -> str:
        return list_to_surql_str(self.value)


class RawRecordId(RecordId[str]):
    # def __init__(self, record_id: str):
    #     if (
    #         record_id.startswith("⟨")
    #         and record_id.endswith("⟩")
    #         and not record_id.endswith("\⟩")
    #     ):
    #         record_id = record_id[1:-1]
    #     super().__init__(record_id)

    def __msgpack__(self) -> str:
        return self.value
