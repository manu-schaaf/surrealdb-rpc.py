import json

from ulid import encode_random, ulid
from uuid_extensions import uuid7str

from surrealdb_rpc.data_model.string import EscapedString, String
from surrealdb_rpc.data_model.surql import (
    dict_to_surql_str,
    list_to_surql_str,
)
from surrealdb_rpc.serialization.abc import JSONSerializable, SurrealQLSerializable


class InvalidRecordIdType(ValueError):
    def __init__(self, invalid: type):
        super().__init__(
            "Valid record ID types are: str, int, list | tuple, and dict."
            f" Got: {invalid.__name__}"
        )


class RecordId[T](JSONSerializable, SurrealQLSerializable):
    def __init__(self, record_id: "T | RecordId[T]"):
        if isinstance(record_id, RecordId):
            self.value: T = record_id.value
        else:
            self.value = record_id

    @classmethod
    def new(
        cls,
        record_id: T,
    ) -> "TextRecordId | NumericRecordId | ObjectRecordId | ArrayRecordId":
        """
        Create a new typed RecordId object. The type is inferred from the `record_id` argument.

        Note:
            Supported types:
            - `TextRecordId`: `str`
            - `NumericRecordId`: `int`
            - `ArrayRecordId`: `list` | `tuple`
            - `ObjectRecordId`: `dict`

            Also note, that this method will *not* coerce a string into `NumericRecordId` if it's numeric, use `parse()` for that instead.

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
                return ArrayRecordId(list(ll))
            case dd if isinstance(dd, dict):
                return ObjectRecordId(dd)
            case _:
                raise InvalidRecordIdType(type(record_id))

    @classmethod
    def parse(
        cls, string: str
    ) -> "TextRecordId | NumericRecordId | ObjectRecordId | ArrayRecordId":
        match string:
            case s if s.isnumeric():
                return NumericRecordId(int(s))
            case dd if dd.startswith("{") and dd.endswith("}"):
                raise NotImplementedError(
                    "Parsing object record IDs not yet implemented."
                )
            case ll if ll.startswith("[") and ll.endswith("]"):
                raise NotImplementedError(
                    "Parsing array record IDs not yet implemented."
                )
            case _:
                if (
                    string.startswith("⟨")
                    and string.endswith("⟩")
                    and not string.endswith(r"\⟩")
                ) or (
                    string.startswith("`")
                    and string.endswith("`")
                    and not string.endswith(r"\`")
                ):
                    string = string[1:-1]
                return TextRecordId(string)

    @classmethod
    def from_surql(cls, string: str) -> "SurrealQLRecordId":
        return SurrealQLRecordId(string)

    @classmethod
    def rand(cls) -> "TextRecordId":
        """Generate a 20-character (a-z0-9) record ID."""
        return TextRecordId(encode_random(20).lower())

    @classmethod
    def ulid(cls) -> "TextRecordId":
        """Generate a ULID-based record ID."""
        return TextRecordId(ulid().lower())

    @classmethod
    def uuid(cls, ns: int | None = None) -> "TextRecordId":
        """Generate a UUIDv7-based record ID."""
        return TextRecordId(uuid7str(ns))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value})"

    def __json__(self):
        return json.dumps(self.value)

    def __surql__(self) -> str:
        match self.value:
            case s if isinstance(s, str):
                return TextRecordId.__surql__(self)  # type: ignore
            case i if isinstance(i, int):
                return NumericRecordId.__surql__(self)  # type: ignore
            case ll if isinstance(ll, (list, tuple)):
                return ArrayRecordId.__surql__(self)  # type: ignore
            case dd if isinstance(dd, dict):
                return ObjectRecordId.__surql__(self)  # type: ignore
            case _:
                raise NotImplementedError

    def __eq__(self, other) -> bool:
        return isinstance(other, RecordId) and self.__surql__() == other.__surql__()


class TextRecordId(RecordId[str]):
    def __surql__(self) -> str:
        if self.value.isnumeric():
            return EscapedString.angle(self.value)
        return String.auto_escape(self.value)


class NumericRecordId(RecordId[int]):
    def __surql__(self):
        return str(self.value)


class ObjectRecordId(RecordId[dict]):
    def __surql__(self) -> str:
        return dict_to_surql_str(self.value)


class ArrayRecordId(RecordId[list]):
    def __surql__(self) -> str:
        return list_to_surql_str(self.value)


class SurrealQLRecordId(RecordId[str]):
    def __surql__(self) -> str:
        return self.value
