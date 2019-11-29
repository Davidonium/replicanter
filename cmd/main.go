package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/davidonium/replicanter"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
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

	cols, err := replicanter.GetColumnNameMap(db, Database)

	panicOnErr(err)

	for {
		bev, _ := streamer.GetEvent(context.Background())

		rev, ok := bev.Event.(*replication.RowsEvent)

		if ok {
			evt := bev.Header.EventType
			action, ok := replicanter.GetActionFromEventType(evt)

			if !ok {
				continue
			}

			schema := string(rev.Table.Schema)
			if schema != Database {
				continue
			}

			table := string(rev.Table.Table)

			if action == replicanter.UpdateAction {
				l := len(rev.Rows)

				if l%2 != 0 {
					errors.New("invalid number of rows for an update, it has to be even (before and after row for each statement)")
				}

				for i := 0; i < l; i += 2 {
					br := replicanter.RowDataFromBinlog(table, cols, rev.Rows[i])
					ar := replicanter.RowDataFromBinlog(table, cols, rev.Rows[i+1])

					br.Dump(os.Stdout)
					ar.Dump(os.Stdout)
				}
			} else {
				for _, row := range rev.Rows {
					r := replicanter.RowDataFromBinlog(table, cols, row)
					r.Dump(os.Stdout)
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
