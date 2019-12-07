package main

import (
	"os"

	"github.com/davidonium/replicanter"
)

type PrintStatementOnRow struct{}

func (*PrintStatementOnRow) Handle(st replicanter.RowStatement) {
	st.Dump(os.Stdout)
}

func main() {
	cfg := replicanter.Config{
		ServerID: 1,
		Host:     "127.0.0.1",
		Port:     3308,
		User:     "root",
		Password: "repl",
		Database: "repl",
	}
	r := replicanter.NewReplicanter(cfg)

	r.OnRow(&PrintStatementOnRow{})

	err := r.Run()

	if err != nil {
		panic(err)
	}
}
