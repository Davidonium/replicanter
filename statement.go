package replicanter

import (
	"fmt"
	"io"
)

type RowData map[string]interface{}

func (rd RowData) Dump(w io.Writer) {
	for key, value := range rd {
		fmt.Fprintf(w, "column - %s: %#v\n", key, value)
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

type UpdatedRowPair struct {
	BeforeRow RowData
	AfterRow  RowData
}

func (urp UpdatedRowPair) Dump(w io.Writer) {
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
	UpdateRows []UpdatedRowPair
}

func (rs RowStatement) Dump(w io.Writer) {
	fmt.Fprintf(w, "schema: %s\n", rs.Schema)
	fmt.Fprintf(w, "table: %s\n", rs.Table)
	fmt.Fprintf(w, "statement: %s\n", SqlActionNames[rs.Action])

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
