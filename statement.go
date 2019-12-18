package replicanter

import (
	"fmt"
	"io"

	"github.com/siddontang/go-mysql/schema"
)

type RowData map[string]interface{}

func (rd RowData) Dump(w io.Writer) {
	fmt.Fprint(w, "start data ==================\n")
	for key, value := range rd {
		fmt.Fprintf(w, "%s: %#v\n", key, value)
	}
	fmt.Fprint(w, "end data ==================\n")
}

func RowDataFromBinlog(table string, tables Tables, row []interface{}) RowData {
	rd := RowData{}

	for colIndex, value := range row {
		c := tables[table].Columns[colIndex]
		colName := c.Name

		if c.Type == schema.TYPE_ENUM {
			value = c.EnumValues[value.(int64)-1]
		}

		rd[colName] = value
	}

	return rd
}

type UpdateRowPair struct {
	BeforeRow RowData
	AfterRow  RowData
}

func (urp UpdateRowPair) Dump(w io.Writer) {
	fmt.Fprintln(w, "--- before ---")
	urp.BeforeRow.Dump(w)

	fmt.Fprintln(w, "--- after ---")
	urp.AfterRow.Dump(w)
}

type RowStatement struct {
	Table      *schema.Table
	Action     SqlAction
	Rows       []RowData
	UpdateRows []UpdateRowPair
}

func (rs RowStatement) Dump(w io.Writer) {
	fmt.Fprintf(w, "schema: %#v | table: %#v | action: %#v\n", rs.Table.Schema, rs.Table.Name, rs.Action.String())
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
