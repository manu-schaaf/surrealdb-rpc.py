# SurrealDB RPC Python Client

## Example Usage

```python
from surrealdb_rpc.client.websocket import SurrealDBClient
from surrealdb_rpc.data_model import RecordId

with SurrealDBClient(
    host="localhost",
    port=8000,
    ns="test",
    db="test",
    user="root",
    password="root",
) as db:
    response = db.create(
        "example:123",
        text="Some value",
        reference=RecordId.new("other", {"foo": {"bar": "baz"}}),
        array=[1, 2, 3],
        object={"key": "value"},
    )
    print(response)
```

This should create this record in the database:

```json
{
  array: [
    1,
    2,
    3
  ],
  id: example:123,
  object: {
    key: 'value'
  },
  reference: other:{
  foo: {
    bar: 'baz'
  }
  },
  text: 'Some value'
}
```
