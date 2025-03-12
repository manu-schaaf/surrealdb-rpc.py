from surrealdb_rpc.data_model.record_id import (
    ArrayRecordId,
    NumericRecordId,
    ObjectRecordId,
    RecordId,
    TextRecordId,
)
from surrealdb_rpc.data_model.string import (
    EscapedString,
    String,
)
from surrealdb_rpc.data_model.table import (
    Table,
)
from surrealdb_rpc.data_model.thing import (
    Thing,
)
from surrealdb_rpc.data_model.types import (
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
    "SingleThing",
    "SingleTable",
    "String",
    "SurrealTypes",
    "Table",
    "TextRecordId",
    "Thing",
    "UUID",
]
