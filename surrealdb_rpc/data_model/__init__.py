from .ext_types import UUID, DateTime, Decimal, Duration
from .thing import (
    ArrayRecordId,
    EscapedString,
    ExtTypes,
    NumericRecordId,
    ObjectRecordId,
    RecordId,
    RecordIdStr,
    String,
    SurrealTypes,
    Table,
    TableNameStr,
    TextRecordId,
    Thing,
    is_record_id_str,
    is_table_name_str,
    pack_record_id,
)

type SingleTable = TableNameStr | Table
type SingleRecordId = RecordIdStr | RecordId
type SingleOrListOfRecordIds = SingleTable | list[SingleRecordId]


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
    "RecordIdStr",
    "SingleOrListOfRecordIds",
    "SingleRecordId",
    "SingleTable",
    "String",
    "SurrealTypes",
    "Table",
    "TableNameStr",
    "TextRecordId",
    "Thing",
    "UUID",
    "is_record_id_str",
    "is_table_name_str",
    "pack_record_id",
]
