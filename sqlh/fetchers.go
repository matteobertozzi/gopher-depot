/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlh

import (
	"database/sql"
)

func sqlAllocReadValues(numFields int) []any {
	values := make([]any, numFields)
	for i := range values {
		values[i] = new(any)
	}
	return values
}

func sqlRowToMap(values []any, numFields int, fields []string) map[string]any {
	rowMap := make(map[string]any, numFields)
	for i, fieldName := range fields {
		value := *(values[i].(*any))
		if bytesVal, ok := value.([]byte); ok {
			rowMap[fieldName] = string(bytesVal)
		} else {
			rowMap[fieldName] = value
		}
	}
	return rowMap
}

func SqlFetchRowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	numFields := len(fields)
	values := sqlAllocReadValues(numFields)

	var results []map[string]any
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		rowMap := sqlRowToMap(values, numFields, fields)
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func SqlFetchSingleRowToMap(rows *sql.Rows) (map[string]any, error) {
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	numFields := len(fields)
	values := sqlAllocReadValues(numFields)
	if err := rows.Scan(values...); err != nil {
		return nil, err
	}

	rowMap := sqlRowToMap(values, numFields, fields)
	return rowMap, nil
}

type SqlFlatRows struct {
	Fields []string `json:"fields"`
	Values []any    `json:"values"`
}

func (rows *SqlFlatRows) HasRows() bool {
	return len(rows.Values) != 0
}

func (rows *SqlFlatRows) IsEmpty() bool {
	return len(rows.Values) == 0
}

func (rows *SqlFlatRows) RowCount() int {
	return len(rows.Values) / len(rows.Fields)
}

func (rows *SqlFlatRows) LastRowFieldValue(fieldName string) any {
	for i, field := range rows.Fields {
		if field == fieldName {
			numFields := len(rows.Fields)
			return rows.Values[len(rows.Values)-numFields+i]
		}
	}
	return nil
}

func (rows *SqlFlatRows) LastRowFieldUint64Value(fieldName string) uint64 {
	value := rows.LastRowFieldValue(fieldName)
	switch v := value.(type) {
	case uint64:
		return v
	case int64:
		return uint64(v)
	case int32:
		return uint64(v)
	case uint32:
		return uint64(v)
	case int16:
		return uint64(v)
	case uint16:
		return uint64(v)
	case int8:
		return uint64(v)
	case uint8:
		return uint64(v)
	case int:
		return uint64(v)
	case uint:
		return uint64(v)
	}
	return 0
}

func SqlFetchRowsToFlatMapWithRsFields(rows *sql.Rows) (*SqlFlatRows, error) {
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	return SqlFetchRowsFlatMap(rows, fields)
}

func SqlFetchRowsFlatMap(rows *sql.Rows, fields []string) (*SqlFlatRows, error) {
	defer rows.Close()

	numFields := len(fields)
	values := sqlAllocReadValues(numFields)

	rowsValues := make([]any, 0, numFields*100)
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		for i := range numFields {
			value := *(values[i].(*any))
			if value == nil {
				rowsValues = append(rowsValues, nil)
			} else if bytesVal, ok := value.([]byte); ok {
				rowsValues = append(rowsValues, string(bytesVal))
			} else {
				rowsValues = append(rowsValues, value)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &SqlFlatRows{
		Fields: fields,
		Values: rowsValues,
	}, nil
}
