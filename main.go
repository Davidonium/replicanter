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

	if err != nil {
		panic(err)
	}

	ds := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", ds)

	colSql := `select TABLE_NAME, 
					  COLUMN_NAME, 
				      ORDINAL_POSITION 
			  	 from COLUMNS
			  	where TABLE_SCHEMA = 'repl'`
	colResult, err := db.Query(colSql)

	if err != nil {
		panic(err)
	}

	var cols = ColumnNameMap{}

	for i := 0; colResult.Next(); i++ {
		var col Column
		err := colResult.Scan(&col.TableName, &col.Name, &col.Ordinal)

		if err != nil {
			panic(err)
		}
		if _, ok := cols[col.TableName]; !ok {
			cols[col.TableName] = map[int]string{}
		}

		cols[col.TableName][col.Ordinal] = col.Name
	}

	err = colResult.Close()

	if err != nil {
		panic(err)
	}

	for {
		ev, _ := streamer.GetEvent(context.Background())

		evt := ev.Header.EventType

		switch evt {
		case replication.UPDATE_ROWS_EVENTv2:
			fallthrough
		case replication.DELETE_ROWS_EVENTv2:
			fallthrough
		case replication.WRITE_ROWS_EVENTv2:
			rev, _ := ev.Event.(*replication.RowsEvent)
			schema := string(rev.Table.Schema)
			table := string(rev.Table.Table)

			if schema != "repl" {
				continue
			}

			fmt.Printf("table: %s\n", table)
			for _, row := range rev.Rows {
				for colIndex, value := range row {
					if _, ok := value.([]byte); ok {
						fmt.Printf("col index: %d | column: %s | value: %q\n", colIndex, cols[table][colIndex + 1], value)
					} else {
						fmt.Printf("col index: %d | column: %s | value: %#v\n", colIndex, cols[table][colIndex + 1], value)
					}
				}
			}

			fmt.Print("\n")

		default:
			continue
		}

	}
}
