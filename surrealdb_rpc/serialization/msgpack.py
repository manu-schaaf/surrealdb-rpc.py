import datetime
import decimal

import msgpack

from surrealdb_rpc.data_model import UUID, DateTime, Decimal, Duration, Thing
from surrealdb_rpc.serialization.abc import SurrealQLSerializable


def msgpack_encode(obj):
    match obj:
        case None:
            return msgpack.ExtType(1, b"")
        case uuid if isinstance(uuid, UUID):
            return msgpack.ExtType(2, uuid.encode("utf-8"))
        case i if isinstance(i, (decimal.Decimal, Decimal)):
            return msgpack.ExtType(3, str(i).encode("utf-8"))
        case td if isinstance(td, (datetime.timedelta, Duration)):
            return msgpack.ExtType(4, Duration.to_string(td).encode("utf-8"))
        case dt if isinstance(dt, (datetime.datetime, DateTime)):
            return msgpack.ExtType(5, DateTime.to_string(dt).encode("utf-8"))
        case thing if isinstance(thing, Thing):
            return msgpack.ExtType(6, thing.__surql__().encode("utf-8"))
        case s if isinstance(s, SurrealQLSerializable):
            return s.__surql__()
        case mapping if isinstance(obj, dict):
            return {msgpack_encode(k): msgpack_encode(v) for k, v in mapping.items()}
        case array if isinstance(obj, (list, tuple)):
            return [msgpack_encode(item) for item in array]
        case _:
            return obj


def msgpack_decode(code, data):
    match code:
        case 1:  # NONE
            return None
        case 2:  # UUID
            return UUID(data.decode("utf-8"))
        case 3:  # Decimal
            return Decimal.from_surql(data.decode("utf-8"))
        case 4:  # Duration
            return Duration.from_surql(data.decode("utf-8"))
        case 5:  # DateTime
            return DateTime.from_surql(data.decode("utf-8"))
        case 6:  # SurrealDB record ID
            return Thing.from_surql(data.decode("utf-8"))
        case _:
            raise ValueError(f"Unknown msgpack extension code: {code}")
