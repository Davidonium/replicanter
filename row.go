package replicanter

import (
	"fmt"
	"io"
)

type RowData map[string]interface{}

func (rd RowData) Dump(w io.Writer) {
	for key, value := range rd {
		fmt.Fprintf(w, "name: %s | value: %#v\n", key, value)
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
