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

package tashkewey

import (
	"context"
	"errors"
)

var (
	basicAuthTokens = map[string]string{}
	basicAuthRoles  = map[string]map[string][]string{}
)

func AddBasicAuthToken(token string, subject string, roles map[string][]string) {
	basicAuthTokens[token] = subject
	basicAuthRoles[token] = roles
}

func RemoveBasicAuthToken(token string) {
	delete(basicAuthTokens, token)
	delete(basicAuthRoles, token)
}

func VerifyBasicAuthToken(ctx context.Context, session *AuthSession, authToken string) error {
	if session == nil {
		return errors.New("session cannot be nil")
	}

	if authToken == "" {
		return errors.New("auth token cannot be empty")
	}

	subject, ok := basicAuthTokens[authToken]
	if !ok {
		return errors.New("invalid basic auth token: " + authToken)
	}

	session.Issuer = "basic"
	session.Subject = subject
	session.Roles = basicAuthRoles[authToken]
	return nil
}
