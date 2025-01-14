from surrealdb_rpc.data_model import (
    RecordId,
    RecordIdStr,
    Table,
    TableNameStr,
)

type SingleTable = TableNameStr | Table
type SingleRecordId = RecordIdStr | RecordId
type SingleOrListOfRecordIds = SingleTable | list[SingleRecordId]
