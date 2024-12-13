import json
import warnings
from abc import ABC, abstractmethod
from collections import UserString
from datetime import datetime, timedelta, timezone
from decimal import Decimal as _Decimal
from typing import Any, Self
from uuid import uuid4

from ulid import encode_random, ulid
from uuid_extensions import uuid7str


class String(UserString):
    @classmethod
    def auto_escape(cls, s: str, use_backtick=False) -> str:
        """Automatically escape a string using the appropriate method.

        Examples:
            >>> String.auto_escape("simple_string")
            'simple_string'
            >>> String.auto_escape("complex-string")
            '⟨complex-string⟩'
            >>> String.auto_escape("complex-string", use_backtick=True)
            '`complex-string`'
        """
        if cls.is_simple(s):
            return s
        return cls.escape_backtick(s) if use_backtick else cls.escape_angle(s)

    @classmethod
    def auto_quote(cls, s: str, use_backtick=False) -> str:
        """Automatically quote a string using double quotes

        Examples:
            >>> String.auto_quote("simple_string")
            '"simple_string"'
            >>> String.auto_quote("complex-string")
            '⟨complex-string⟩'
            >>> String.auto_quote("complex-string", use_backtick=True)
            '`complex-string`'
        """
        if cls.is_simple(s):
            return f'"{s}"'
        return cls.escape_backtick(s) if use_backtick else cls.escape_angle(s)

    @classmethod
    def escape_angle(cls, s: str) -> str:
        """Escape a string using angle brackets.

        Examples:
            >>> String.escape_angle("simple_string")
            '⟨simple_string⟩'
            >>> String.escape_angle("complex-string")
            '⟨complex-string⟩'
        """
        return EscapedString.angle(s)

    @classmethod
    def escape_backtick(cls, s: str) -> str:
        """Escape a string using backticks.

        Examples:
            >>> String.escape_backtick("simple_string")
            '`simple_string`'
            >>> String.escape_backtick("complex-string")
            '`complex-string`'
        """
        return EscapedString.backtick(s)

    @classmethod
    def _is_simple_char(cls, c: str) -> bool:
        return c.isalnum() or c == "_"

    @classmethod
    def is_simple(cls, s: str) -> bool:
        return all(map(String._is_simple_char, s))


class EscapedString(String):
    @classmethod
    def angle(cls, string) -> Self:
        if not isinstance(string, cls):
            if string.startswith("⟨") and string.endswith("⟩"):
                warnings.warn(
                    f"The string {string} is already angle-escaped, are you sure you want to escape it again?"
                )
            return cls(f"⟨{string}⟩")
        return cls(string)

    @classmethod
    def backtick(cls, string) -> Self:
        if not isinstance(string, cls):
            if string.startswith("`") and string.endswith("`"):
                warnings.warn(
                    f"The string {string} is already backtick-escaped, are you sure you want to escape it again?"
                )
            return cls(f"`{string}`")
        return cls(string)


class Thing[R](ABC):
    __reference_class__: type[R]

    @classmethod
    def from_str(cls, s: str, escaped: bool = False) -> "Table | RecordId":
        if ":" in s:
            table, id = s.split(":", 1)
            if not id:
                raise ValueError(
                    f"Expected RecordId as string '{s}' contains a colon, but ID was empty!"
                )
            return RecordId.new(table, EscapedString(id) if escaped else id)
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
            raise ValueError(
                f"Cannot convert object of type {type(obj).__name__} into a Thing: {str(obj)}"
            )

    @abstractmethod
    def __iter__(self): ...
    @abstractmethod
    def __json__(self) -> str: ...
    @abstractmethod
    def __pack__(self) -> str: ...


class Table(Thing):
    def __init__(self, table: str | Self):
        self.table = table.table if isinstance(table, type(self)) else table

    def __post_init__(self):
        if not String.is_simple(self.table):
            raise ValueError(
                f"Table names must be simple strings and can only contain ASCII letters, numbers, and underscores. Got: {self.table}"
            )

    def __repr__(self):
        return f"{type(self).__name__}({self.table})"

    def __iter__(self):
        yield self.table

    def __json__(self):
        return self.table

    def __pack__(self) -> str:
        return self.table

    def change_table(self, table):
        self.table = table


class RecordId[T](Table):
    def __init__(self, table: str, id: T):
        super().__init__(table)
        self.id: T = id

    def __repr__(self):
        return f"{type(self).__name__}({self.table}:{str(self.id)})"

    def __iter__(self):
        yield self.table
        yield self.id

    def __json__(self):
        return f"{self.table}:{json.dumps(self.id)}"

    def __pack__(self) -> str:
        return f"{self.table}:{pack_record_id(self.id)}"

    @classmethod
    def new(
        cls,
        table: str | Table,
        id: T,
    ) -> "TextRecordId | NumericRecordId | ObjectRecordId | ArrayRecordId":
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
                raise TypeError(f"Unsupported record ID type: {type(id)}")

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
    def __init__(self, table: str | Table, id: str):
        super().__init__(table, String.auto_escape(id))


class NumericRecordId(RecordId[int]):
    pass


class ObjectRecordId(RecordId[dict]):
    def __pack__(self) -> str:
        id_str = "{"
        ",".join(
            String.auto_escape(k) + ":" + pack_record_id(v) for k, v in self.id.items()
        )
        id_str += "}"
        return f"{self.table}:{id_str}"


class ArrayRecordId(RecordId[list]):
    pass


class Duration(timedelta):
    def __str__(self) -> str:
        string = ""
        if self.days:
            string += f"{self.days}d"
        if self.seconds:
            string += f"{self.seconds}s"
        if self.microseconds:
            string += f"{self.microseconds}us"
        return string

    @classmethod
    def from_str(cls, string: str) -> Self:
        weeks = 0
        days = 0
        hours = 0
        minutes = 0
        seconds = 0
        milis = 0
        micros = 0
        nanos = 0

        if "y" in string:
            raise NotImplementedError("Years are not supported in Duration")
            # years, string = string.split("y")
            # years = int(years)
        if "w" in string:
            weeks, string = string.split("w")
            weeks = int(weeks)
        if "d" in string:
            days, string = string.split("d")
            days = int(days)
        if "h" in string:
            hours, string = string.split("h")
            hours = int(hours)
        if "m" in string:
            minutes, string = string.split("m")
            minutes = int(minutes)
        if "s" in string:
            seconds, string = string.split("s")
            seconds = int(seconds)
        if "ms" in string:
            milis, string = string.split("ms")
            milis = int(milis)
        if "us" in string:
            micros, string = string.split("us")
            micros = int(micros)
        elif "µs" in string:
            micros, string = string.split("µs")
            micros = int(micros)
        if "ns" in string:
            warnings.warn(
                "Nanoseconds are not supported in Duration, converting to microseconds"
            )
            nanos, string = string.split("ns")
            nanos = int(nanos) // 1000
            micros += nanos

        return cls(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milis,
            microseconds=micros,
        )

    @classmethod
    def __decode__(cls, data: bytes) -> Self:
        return cls.from_str(data.decode("utf-8"))


class DateTime(datetime):
    def __str__(self) -> str:
        return self.astimezone(timezone.utc).isoformat()

    @classmethod
    def from_str(cls, string: str) -> Self:
        return cls.fromisoformat(string)

    @classmethod
    def __decode__(cls, data: bytes) -> Self:
        return cls.from_str(data.decode("utf-8"))


class Decimal(_Decimal):
    @classmethod
    def from_str(cls, string: str) -> Self:
        return cls(string.removesuffix("dec"))

    @classmethod
    def __decode__(cls, data: bytes) -> Self:
        return cls.from_str(data.decode("utf-8"))


class UUID(UserString):
    def __init__(self, value: str | bytes | None):
        if value is None:
            value = self.new()
        elif isinstance(value, bytes):
            value = value.decode("utf-8")
        super().__init__(value)

    @classmethod
    def new(cls) -> Self:
        return cls.new_v7()

    @classmethod
    def new_v4(cls) -> Self:
        return cls(uuid4().hex)

    @classmethod
    def new_v7(cls) -> Self:
        return cls(uuid7str())


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
