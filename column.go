package replicanter

import "database/sql"

type Column struct {
	Table   string
	Name    string
	Ordinal int
}

// ColumnNames table --> column ordinal = column name
type ColumnNames map[string]map[int]string

type SqlColumnNamesRetriever struct {
	db *sql.DB
}

func NewSqlColumnNamesRetriever(db *sql.DB) *SqlColumnNamesRetriever {
	return &SqlColumnNamesRetriever{db: db}
}

func (cnr *SqlColumnNamesRetriever) All(schema string) (ColumnNames, error) {
	colSql := `select table_name, 
					  column_name, 
				      ordinal_position 
			  	 from information_schema.columns
			  	where table_schema = ?`
	colResult, err := cnr.db.Query(colSql, schema)

	if err != nil {
		return nil, err
	}

	defer colResult.Close()

	var cols = ColumnNames{}

	for i := 0; colResult.Next(); i++ {
		var col Column
		err := colResult.Scan(&col.Table, &col.Name, &col.Ordinal)

		if err != nil {
			return nil, err
		}

		if _, ok := cols[col.Table]; !ok {
			cols[col.Table] = map[int]string{}
		}

		cols[col.Table][col.Ordinal] = col.Name
	}

	return cols, nil
}
