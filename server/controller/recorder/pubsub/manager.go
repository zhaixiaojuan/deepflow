/**
 * Copyright (c) 2023 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pubsub

import (
	"errors"
	"sync"
)

var (
	pubSubManagerOnce sync.Once
	pubSubManager     *Manager
)

func GetManager() *Manager {
	pubSubManagerOnce.Do(func() {
		pubSubManager = &Manager{
			TypeToPubSub: map[int]PubSub{
				PubSubTypeAZ:           NewAZ(),
				PubSubTypeRegion:       NewRegion(),
				PubSubTypeVM:           NewVM(),
				PubSubTypeSubnet:       NewSubnet(),
				PubSubTypeWANIP:        NewWANIP(),
				PubSubTypeLANIP:        NewLANIP(),
				PubSubTypePodGroupPort: NewPodGroupPort(),
				PubSubTypePod:          NewPod(),
				PubSubTypeProcess:      NewProcess(),
			},
		}
	})
	return pubSubManager
}

type Manager struct {
	TypeToPubSub map[int]PubSub
}

func Subscribe(pubSubType, topic int, handler interface{}) error {
	ps, ok := GetManager().TypeToPubSub[pubSubType]
	if !ok {
		log.Errorf("pubsub type not found: %d", pubSubType)
		return errors.New("pubsub type not found")
	}
	ps.Subscribe(topic, handler)
	return nil
}

func GetPubSub(pubSubType int) interface{} {
	ps, ok := GetManager().TypeToPubSub[pubSubType]
	if !ok {
		return nil
	}
	return ps
}
