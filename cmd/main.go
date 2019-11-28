package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type Column struct {
	TableName string
	Name      string
	Ordinal   int
}

// ColumnNameMap tablename --> column ordinal = column name
type ColumnNameMap = map[string]map[int]string

type RowData = map[string]interface{}

type SqlAction int

const (
	InsertAction SqlAction = iota
	UpdateAction
	DeleteAction
)

const Database = "repl"

func main() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3308,
		User:     "root",
		Password: "repl",
	}
	syncer := replication.NewBinlogSyncer(cfg)

	// TODO retrieve last position using `show master status` query and provide it as an option
	streamer, err := syncer.StartSync(mysql.Position{Name: "", Pos: 0})

	panicOnErr(err)

	ds := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", ds)

	cols, err := GetColumnNameMap(db)

	panicOnErr(err)

	for {
		bev, _ := streamer.GetEvent(context.Background())

		rev, ok := bev.Event.(*replication.RowsEvent)

		if ok {
			evt := bev.Header.EventType
			action, ok := GetActionFromEventType(evt)

			if !ok {
				continue
			}

			schema := string(rev.Table.Schema)
			if schema != Database {
				continue
			}

			table := string(rev.Table.Table)

			if action == UpdateAction {
				l := len(rev.Rows)

				if l%2 != 0 {
					errors.New("invalid number of rows for an update, it has to be even (before and after row for each statement)")
				}

				for i := 0; i < l; i += 2 {
					br := RowDataFromBinlog(table, cols, rev.Rows[i])
					ar := RowDataFromBinlog(table, cols, rev.Rows[i+1])
				}
			} else {
				for _, row := range rev.Rows {
					r := RowDataFromBinlog(table, cols, row)
				}
			}

			fmt.Print("\n")
		}
	}
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

var RowActionNames = map[SqlAction]string{
	InsertAction: "insert",
	UpdateAction: "update",
	DeleteAction: "delete",
}

var eventTypeToAction = map[replication.EventType]SqlAction{
	replication.WRITE_ROWS_EVENTv2:  InsertAction,
	replication.UPDATE_ROWS_EVENTv2: UpdateAction,
	replication.DELETE_ROWS_EVENTv2: DeleteAction,
}

func GetActionFromEventType(evt replication.EventType) (SqlAction, bool) {
	action, ok := eventTypeToAction[evt]
	return action, ok
}

func GetColumnNameMap(db *sql.DB) (ColumnNameMap, error) {
	colSql := `select table_name, 
					  column_name, 
				      ordinal_position 
			  	 from columns
			  	where table_schema = ?`
	colResult, err := db.Query(colSql, Database)

	if err != nil {
		return nil, err
	}

	var cols = ColumnNameMap{}

	for i := 0; colResult.Next(); i++ {
		var col Column
		err := colResult.Scan(&col.TableName, &col.Name, &col.Ordinal)

		if err != nil {
			return nil, err
		}

		if _, ok := cols[col.TableName]; !ok {
			cols[col.TableName] = map[int]string{}
		}

		cols[col.TableName][col.Ordinal] = col.Name
	}

	err = colResult.Close()

	if err != nil {
		return nil, err
	}

	return cols, nil
}

func RowDataFromBinlog(table string, cols ColumnNameMap, row []interface{}) RowData {
	rd := RowData{}
	for colIndex, value := range row {
		colName := cols[table][colIndex+1]
		rd[colName] = value
	}

	return rd
}
