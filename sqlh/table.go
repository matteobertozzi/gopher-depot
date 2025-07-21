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
	"fmt"
	"iter"
)

type SqlScanner interface {
	Scan(dest ...any) error
}

type SqlRowLoader interface {
	LoadFromSqlRow(row SqlScanner) error
}

type SqlTable[T SqlRowLoader] struct {
	db   *sql.DB
	name string
}

func NewSqlTable[T SqlRowLoader](db *sql.DB, tableName string) *SqlTable[T] {
	return &SqlTable[T]{
		db:   db,
		name: tableName,
	}
}

func (table *SqlTable[T]) FetchRow(sql string, args ...any) (*T, error) {
	row := table.db.QueryRow(sql, args...)

	var item T
	if err := item.LoadFromSqlRow(row); err != nil {
		return nil, fmt.Errorf("load from row of table:%s failed: %w", table.name, err)
	}
	return &item, nil
}

func (table *SqlTable[T]) FetchAll(sql string, args ...any) ([]T, error) {
	rows, err := table.db.Query(sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []T
	for rows.Next() {
		var item T
		if err := item.LoadFromSqlRow(rows); err != nil {
			return nil, fmt.Errorf("load from row of table:%s failed: %w", table.name, err)
		}
		results = append(results, item)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration of table:%s failed: %w", table.name, err)
	}

	return results, nil
}

func (table *SqlTable[T]) FetchIter(sql string, args ...any) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		rows, err := table.db.Query(sql, args...)
		if err != nil {
			yield(nil, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item T
			if err := item.LoadFromSqlRow(rows); err != nil {
				yield(nil, fmt.Errorf("load from row of table:%s failed: %w", table.name, err))
				return
			}

			if !yield(&item, nil) {
				return
			}
		}

		if err = rows.Err(); err != nil {
			yield(nil, fmt.Errorf("rows iteration of table:%s failed: %w", table.name, err))
		}
	}
}
