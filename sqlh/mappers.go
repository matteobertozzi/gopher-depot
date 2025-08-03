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
