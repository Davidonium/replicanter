package replicanter

import (
	"fmt"
	"io"
)

type RowData map[string]interface{}

func (rd RowData) Dump(w io.Writer) {
	fmt.Fprint(w, "data: \n")
	for key, value := range rd {
		fmt.Fprintf(w, "%s: %#v\n", key, value)
	}
}

func RowDataFromBinlog(table string, cols ColumnNames, row []interface{}) RowData {
	rd := RowData{}

	for colIndex, value := range row {
		colName := cols[table][colIndex+1]
		rd[colName] = value
	}

	return rd
}

type UpdateRowPair struct {
	BeforeRow RowData
	AfterRow  RowData
}

func (urp UpdateRowPair) Dump(w io.Writer) {
	fmt.Fprintln(w, "before:")
	urp.BeforeRow.Dump(w)

	fmt.Fprintln(w, "after:")
	urp.AfterRow.Dump(w)
}

type RowStatement struct {
	Schema     string
	Table      string
	Action     SqlAction
	Rows       []RowData
	UpdateRows []UpdateRowPair
}

func (rs RowStatement) Dump(w io.Writer) {
	fmt.Fprintf(w, "schema: %#v | table: %#v | action: %#v\n", rs.Schema, rs.Table, SqlActionNames[rs.Action])
	if rs.Action == UpdateAction {
		for _, r := range rs.UpdateRows {
			r.Dump(w)
		}
	} else {
		for _, r := range rs.Rows {
			r.Dump(w)
		}
	}
}
