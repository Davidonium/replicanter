package replicanter

import "github.com/siddontang/go-mysql/replication"

type SqlAction int

const (
	InsertAction SqlAction = iota
	UpdateAction
	DeleteAction
)

func (sa SqlAction) String() string {
	return sqlActionNames[sa]
}

var sqlActionNames = map[SqlAction]string{
	InsertAction: "insert",
	UpdateAction: "update",
	DeleteAction: "delete",
}

var eventTypeToAction = map[replication.EventType]SqlAction{
	replication.WRITE_ROWS_EVENTv1:  InsertAction,
	replication.UPDATE_ROWS_EVENTv1: UpdateAction,
	replication.DELETE_ROWS_EVENTv1: DeleteAction,
	replication.WRITE_ROWS_EVENTv2:  InsertAction,
	replication.UPDATE_ROWS_EVENTv2: UpdateAction,
	replication.DELETE_ROWS_EVENTv2: DeleteAction,
}
