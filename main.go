package main

import (
	"context"
	"database/sql"
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

type RowAction int

const (
	InsertAction RowAction = iota
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

	streamer, err := syncer.StartSync(mysql.Position{Name: "", Pos: 0})

	panicOnErr(err)

	ds := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", ds)

	cols, err := GetColumnNameMap(db)

	panicOnErr(err)

	for {
		ev, _ := streamer.GetEvent(context.Background())

		rev, ok := ev.Event.(*replication.RowsEvent)

		if ok {
			evt := ev.Header.EventType
			action, ok := GetActionFromEventType(evt)

			if !ok {
				continue
			}

			schema := string(rev.Table.Schema)
			table := string(rev.Table.Table)

			if schema != Database {
				continue
			}

			fmt.Printf("action: %s\n", RowActionNames[action])
			fmt.Printf("table: %s\n", table)

			if action == UpdateAction {
				fmt.Println("before row")
				br := MapRowFromBinlog(table, cols, rev.Rows[0])
				fmt.Printf("%#v\n", br)
				fmt.Print("\n")

				fmt.Println("after row")
				ar := MapRowFromBinlog(table, cols, rev.Rows[1])
				fmt.Printf("%#v\n", ar)
			} else {
				for _, row := range rev.Rows {
					r := MapRowFromBinlog(table, cols, row)
					fmt.Printf("%#v\n", r)
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

var RowActionNames = map[RowAction]string{
	InsertAction: "insert",
	UpdateAction: "update",
	DeleteAction: "delete",
}

var eventTypeToAction = map[replication.EventType]RowAction{
	replication.WRITE_ROWS_EVENTv2:  InsertAction,
	replication.UPDATE_ROWS_EVENTv2: UpdateAction,
	replication.DELETE_ROWS_EVENTv2: DeleteAction,
}

func GetActionFromEventType(evt replication.EventType) (RowAction, bool) {
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

func MapRowFromBinlog(table string, cols ColumnNameMap, row []interface{}) RowData {
	rd := RowData{}
	for colIndex, value := range row {
		colName := cols[table][colIndex+1]
		rd[colName] = value
	}

	return rd
}
