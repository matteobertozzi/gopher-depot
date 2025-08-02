package sqlh

import (
	"encoding/json"
	"fmt"
)

func MapSqlFlatRows[T any](sqlFlatRows *SqlFlatRows, yield func(rowIndex int, obj *T, err error) error) error {
	fields := sqlFlatRows.Fields
	rowCount := sqlFlatRows.RowCount()

	row := make(map[string]any, len(fields))
	for rowIndex := range rowCount {
		clear(row)
		for fieldIndex, fieldName := range fields {
			row[fieldName] = sqlFlatRows.Values[(rowIndex*len(fields))+fieldIndex]
		}

		obj, err := mapToObject[T](row)
		if err := yield(rowIndex, obj, err); err != nil {
			return err
		}
	}
	return nil
}

func mapToObject[T any](row map[string]any) (*T, error) {
	// TODO: optimize me

	enc, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("error marshaling row: %v", err)
	}

	var v T
	if err := json.Unmarshal(enc, &v); err != nil {
		return nil, fmt.Errorf("error unmarshalling row: %v", err)
	}

	return &v, nil
}
