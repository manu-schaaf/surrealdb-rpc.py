from surrealdb_rpc.data_model.thing import Thing
from surrealdb_rpc.data_model.types.extension import UUID, DateTime, Decimal, Duration

ExtTypes = None | UUID | Decimal | Duration | DateTime | Thing
SurrealTypes = None | bool | int | float | str | bytes | list | dict | ExtTypes

__all__ = [
    "DateTime",
    "Decimal",
    "Duration",
    "ExtTypes",
    "SurrealTypes",
    "Thing",
    "UUID",
]
