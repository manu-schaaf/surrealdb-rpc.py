from .record_id import (
    ArrayRecordId,
    NumericRecordId,
    ObjectRecordId,
    RecordId,
    TextRecordId,
)
from .string import (
    EscapedString,
    String,
)
from .table import (
    Table,
)
from .thing import (
    Thing,
)
from .types import (
    UUID,
    DateTime,
    Decimal,
    Duration,
    ExtTypes,
    SurrealTypes,
)

type SingleTable = str | Table
type SingleThing = str | Thing
type OneOrManyThings = SingleThing | list[SingleThing]


__all__ = [
    "ArrayRecordId",
    "DateTime",
    "Decimal",
    "Duration",
    "EscapedString",
    "ExtTypes",
    "NumericRecordId",
    "ObjectRecordId",
    "RecordId",
    "SingleOrListOfRecordIds",
    "SingleThing",
    "SingleTable",
    "String",
    "SurrealTypes",
    "Table",
    "TextRecordId",
    "Thing",
    "UUID",
    "is_record_id_str",
    "is_table_name_str",
    "pack_record_id",
]
