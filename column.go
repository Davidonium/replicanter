package replicanter

import (
	"database/sql"

	"github.com/siddontang/go-mysql/schema"
)

// Tables table --> column ordinal = column name
type Tables map[string]*schema.Table

type SqlColumnNamesRetriever struct {
	db *sql.DB
}

func NewSqlColumnNamesRetriever(db *sql.DB) *SqlColumnNamesRetriever {
	return &SqlColumnNamesRetriever{db: db}
}

func (cnr *SqlColumnNamesRetriever) All(ts string) (Tables, error) {
	colSql := `select table_name, 
					  column_name, 
				      ordinal_position,
       				  column_type,
       				  extra,
       				  collation_name
			  	 from information_schema.columns
			  	where table_schema = ?`
	colResult, err := cnr.db.Query(colSql, ts)

	if err != nil {
		return nil, err
	}

	defer colResult.Close()

	var cols = Tables{}

	for i := 0; colResult.Next(); i++ {
		var tableName, colName, colType, extra string
		var colOrdinal int
		var collation sql.NullString
		err := colResult.Scan(
			&tableName,
			&colName,
			&colOrdinal,
			&colType,
			&extra,
			&collation,
		)

		if err != nil {
			return nil, err
		}

		if _, ok := cols[tableName]; !ok {
			cols[tableName] = &schema.Table{
				Schema:  ts,
				Name:    tableName,
				Columns: make([]schema.TableColumn, 0, 16),
				Indexes: make([]*schema.Index, 0, 8),
			}
		}

		cols[tableName].AddColumn(colName, colType, collation.String, extra)
	}

	return cols, nil
}
