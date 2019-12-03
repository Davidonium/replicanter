package replicanter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type ColumnNamesRetriever interface {
	All(schema string) (ColumnNames, error)
}

type OnUpdate interface {
	Handle(st UpdateStatement)
}

type OnDefault interface {
	Handle(st DefaultStatement)
}

type doNothingOnUpdate struct{}

func (*doNothingOnUpdate) Handle(_ UpdateStatement) {}

type doNothingOnDefault struct{}

func (*doNothingOnDefault) Handle(_ DefaultStatement) {}

type Replicanter struct {
	config    Config
	onUpdate  OnUpdate
	onDefault OnDefault
}

func NewReplicanter(cfg Config) *Replicanter {
	return &Replicanter{
		config:    cfg,
		onUpdate:  &doNothingOnUpdate{},
		onDefault: &doNothingOnDefault{},
	}
}

func (r *Replicanter) OnUpdate(update OnUpdate) {
	r.onUpdate = update
}

func (r *Replicanter) OnDefault(def OnDefault) {
	r.onDefault = def
}

func (r *Replicanter) Run() error {
	cfg := replication.BinlogSyncerConfig{
		ServerID: r.config.ServerID,
		Flavor:   "mysql",
		Host:     r.config.Host,
		Port:     r.config.Port,
		User:     r.config.User,
		Password: r.config.Password,
	}

	ds := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", r.config.User, r.config.Password, r.config.Host, r.config.Port, r.config.Database)

	db, err := sql.Open("mysql", ds)

	if err != nil {
		return err
	}

	cnr := NewSqlColumnNamesRetriever(db)

	cols, err := cnr.All(r.config.Database)

	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(cfg)

	// TODO retrieve last position using `show master status` query and provide it as an option
	streamer, err := syncer.StartSync(mysql.Position{Name: "", Pos: 0})

	if err != nil {
		return err
	}

	for {
		bev, _ := streamer.GetEvent(context.Background())

		rev, ok := bev.Event.(*replication.RowsEvent)

		if ok {
			evt := bev.Header.EventType
			action, ok := GetSqlActionFromEventType(evt)

			if !ok {
				continue
			}

			schema := string(rev.Table.Schema)
			if schema != r.config.Database {
				continue
			}

			table := string(rev.Table.Table)

			if action == UpdateAction {
				l := len(rev.Rows)

				if l%2 != 0 {
					return errors.New("invalid number of rows for an update, it has to be even (before and after row for each statement)")
				}

				rows := make([]UpdatedRowPair, l/2)
				for i := 0; i < l; i += 2 {
					br := RowDataFromBinlog(table, cols, rev.Rows[i])
					ar := RowDataFromBinlog(table, cols, rev.Rows[i+1])
					pair := UpdatedRowPair{
						BeforeRow: br,
						AfterRow:  ar,
					}

					rows = append(rows, pair)
				}

				us := UpdateStatement{
					Schema: schema,
					Table:  table,
					Action: action,
					Rows:   rows,
				}

				r.onUpdate.Handle(us)
			} else {
				rows := make([]RowData, len(rev.Rows))
				for _, row := range rev.Rows {
					r := RowDataFromBinlog(table, cols, row)
					rows = append(rows, r)
				}

				s := DefaultStatement{
					Schema: schema,
					Table:  table,
					Action: action,
					Rows:   rows,
				}

				r.onDefault.Handle(s)
			}
		}
	}
	return nil
}
