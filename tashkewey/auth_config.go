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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type AuthConfig struct {
	Basic []BasicAuthConfig `json:"basic"`
	Jwk   []JwkAuthConfig   `json:"jwk"`
}

type BasicAuthConfig struct {
	Subject *string             `json:"subject"`
	Token   string              `json:"token"`
	Roles   map[string][]string `json:"roles"`
}

type JwkAuthConfig struct {
	Issuer   string   `json:"issuer"`
	CertsUri string   `json:"certsUri"`
	Audience []string `json:"audience"`
}

type authConfigFile struct {
	Auth AuthConfig `json:"auth"`
}

func LoadAuthConfigFromJsonFile(path string) error {
	jsonFile, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", path, err)
	}
	defer jsonFile.Close()

	data, err := io.ReadAll(jsonFile)
	if err != nil {
		return fmt.Errorf("unable to read file %s: %v", path, err)
	}

	var config authConfigFile
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("unable to unmarshal json: %v", err)
	}

	return LoadAuthConfig(&config.Auth)
}

func LoadAuthConfig(config *AuthConfig) error {
	if len(config.Basic) > 0 {
		if err := loadBasicAuthConfig(config.Basic); err != nil {
			return fmt.Errorf("unable to load basic auth config: %v", err)
		}
	}
	if len(config.Jwk) > 0 {
		if err := loadJwtAuthConfig(config.Jwk); err != nil {
			return fmt.Errorf("unable to load jwt auth config: %v", err)
		}
	}
	return nil
}

func loadBasicAuthConfig(configs []BasicAuthConfig) error {
	for _, config := range configs {
		if config.Token == "" {
			return errors.New("token is required")
		}

		if config.Subject == nil {
			token, err := base64.StdEncoding.DecodeString(config.Token)
			if err != nil {
				return fmt.Errorf("unable to decode token %s: %v", config.Token, err)
			}
			index := bytes.IndexByte(token, ':')
			if index < 0 {
				config.Subject = &config.Token
			} else {
				subject := config.Token[:index]
				config.Subject = &subject
			}
		}

		tracer.LogTrace(context.Background(), "register {basic.subject}", config.Subject)
		AddBasicAuthToken(config.Token, *config.Subject, config.Roles)
	}
	return nil
}

func loadJwtAuthConfig(configs []JwkAuthConfig) error {
	for _, config := range configs {
		if config.Issuer == "" {
			return errors.New("issuer is required")
		}
		if config.CertsUri == "" {
			return errors.New("certsUri is required")
		}

		tracer.LogTrace(context.Background(), "register {jwt.issuer} {jwt.certs.uri}", config.Issuer, config.CertsUri)
		AddJwksUrl(config.Issuer, config.CertsUri)

		if len(config.Audience) > 0 {
			for _, aud := range config.Audience {
				AddAllowedAudience(aud)
			}
		}
	}
	return nil
}
