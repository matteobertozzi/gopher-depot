
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

package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/matteobertozzi/gopher-depot/tashkewey"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all connections
	},
}

type WebSocketService struct{}

func (s *WebSocketService) RegisterRoutes(mux *http.ServeMux) {
	tashkewey.AddRoute(mux, s.handleWebSocket())
}

func (s *WebSocketService) handleWebSocket() tashkewey.Route {
	return tashkewey.Route{
		Method:             http.MethodGet,
		Uri:                "/ws",
		RequiredPermission: tashkewey.AllowPublic{},
		Handler:            s.webSocketHandler,
	}
}

func (s *WebSocketService) webSocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	log.Println("WebSocket connection established")

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			break
		}
		log.Printf("recv: %s", p)
		err = conn.WriteMessage(messageType, p)
		if err != nil {
			log.Println("write error:", err)
			break
		}
	}
}

func main() {
	// init handlers
	wsService := &WebSocketService{}

	// setup the server
	server := tashkewey.NewTashkewey(":9001", tashkewey.TashkeweyOptions{})

	// register routes/handlers
	wsService.RegisterRoutes(server.Mux())

	log.Println("WebSocket server starting on :9001")
	server.ListenAndServe()
}
