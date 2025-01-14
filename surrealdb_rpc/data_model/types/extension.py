import warnings
from datetime import datetime, timedelta, timezone
from decimal import Decimal as _Decimal
from typing import Self
from uuid import uuid4

from uuid_extensions import uuid7str


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


class UUID(str):
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
