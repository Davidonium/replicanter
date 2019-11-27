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

const (
	InsertAction = "insert"
	UpdateAction = "update"
	DeleteAction = "delete"
	Database     = "repl"
)

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

			fmt.Printf("action: %s - table: %s\n", action, table)
			for _, row := range rev.Rows {
				for colIndex, value := range row {
					colName := cols[table][colIndex+1]
					if _, ok := value.([]byte); ok {
						fmt.Printf("byte array - %s: %q\n", colName, value)
					} else {
						fmt.Printf("%s: %#v\n", colName, value)
					}
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

func GetActionFromEventType(evt replication.EventType) (string, bool) {
	var action string
	switch evt {
	case replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	case replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	default:
		return "", false
	}

	return action, true
}

func GetColumnNameMap(db *sql.DB) (ColumnNameMap, error) {
	colSql := `select TABLE_NAME, 
					  COLUMN_NAME, 
				      ORDINAL_POSITION 
			  	 from COLUMNS
			  	where TABLE_SCHEMA = ?`
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
