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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func FindSeqFiles(dbDirPath string, prefix string, suffix string) ([]string, error) {
	if _, err := os.Stat(dbDirPath); os.IsNotExist(err) {
		return nil, nil
	}

	files, err := os.ReadDir(dbDirPath)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, nil
	}

	var dbFiles []string
	for _, file := range files {
		fileName := file.Name()
		if strings.HasPrefix(fileName, prefix) && strings.HasSuffix(fileName, suffix) {
			dbFiles = append(dbFiles, file.Name())
		}
	}

	sort.Strings(dbFiles)
	return dbFiles, nil
}

func FindLatestSeqFilePath(dbDirPath string, prefix string, suffix string) (string, error) {
	files, err := FindSeqFiles(dbDirPath, prefix, suffix)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", nil
	}

	latestFile := files[len(files)-1]
	latestPath := filepath.Join(dbDirPath, latestFile)
	return latestPath, nil
}

func FindNextSeqFilePath(dbDirPath, prefix string, suffix string) (string, error) {
	files, err := FindSeqFiles(dbDirPath, prefix, suffix)
	if err != nil {
		return "", err
	}

	seqId := 1
	if len(files) != 0 {
		latestFile := files[len(files)-1]
		seqIdPart := latestFile[len(prefix)+1 : len(latestFile)-3]
		parsed, err := strconv.Atoi(seqIdPart)
		if err != nil {
			return "", err
		}
		seqId = parsed + 1
	}

	filename := fmt.Sprintf("%s.%010d.%s", prefix, seqId, suffix)
	return filepath.Join(dbDirPath, filename), nil
}
