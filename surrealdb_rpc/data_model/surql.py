from typing import Any

from surrealdb_rpc.data_model.string import String
from surrealdb_rpc.serialization.abc import SurrealQLSerializable


def list_to_surql_str(ll: list) -> str:
    return f"[{', '.join(obj_to_surql_str(e, True) for e in ll)}]"


def dict_to_surql_str(dd: dict) -> str:
    return (
        "{ "
        + ", ".join(
            f"{String.auto_escape(k, True)}: {obj_to_surql_str(v, True)}"
            for k, v in dd.items()
        )
        + " }"
    )


def obj_to_surql_str(value: Any, quote: bool = False) -> None | str:
    """Convert an object into a SurrealQL-compatible string representation.

    Examples:
        >>> obj_to_surql_str("simple")
        'simple'
        >>> obj_to_surql_str("complex-string")
        '⟨complex-string⟩'
        >>> obj_to_surql_str("complex-string", quote=True)
        "'complex-string'"
        >>> obj_to_surql_str(42)
        '42'
        >>> obj_to_surql_str(["hello", "world"])
        "['hello', 'world']"
        >>> obj_to_surql_str({"simple_key": "value"})
        "{ simple_key: 'value' }"
        >>> obj_to_surql_str({"key": "value", "a": "b"})
        "{ key: 'value', a: 'b' }"
        >>> from surrealdb_rpc.data_model.thing import Thing
        >>> obj_to_surql_str(Thing("table", "id"))
        'table:id'
        >>> obj_to_surql_str(Thing("table", "123"))
        'table:⟨123⟩'
    """
    match value:
        case s if isinstance(s, str):
            return String.auto_quote(s) if quote else String.auto_escape(s)
        case i if isinstance(i, int):
            return str(i)
        case ll if isinstance(ll, (list, tuple)):
            return list_to_surql_str(ll)
        case dd if isinstance(dd, dict):
            return dict_to_surql_str(dd)
        case rid if isinstance(rid, SurrealQLSerializable):
            return rid.__surql__()
        case _:
            raise NotImplementedError
