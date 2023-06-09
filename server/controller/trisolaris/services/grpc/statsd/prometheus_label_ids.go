/*
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

package statsd

import (
	"sync"
	"sync/atomic"
)

var (
	gplidCounterOnce sync.Once
	gplidCounter     *GetPrometheusLabelIDsCounter
)

func GetPrometheusLabelIDsDetailCounter() *GetPrometheusLabelIDsCounter {
	gplidCounterOnce.Do(func() {
		gplidCounter = &GetPrometheusLabelIDsCounter{
			PrometheusLabelIDsCounter: &PrometheusLabelIDsCounter{},
		}
	})
	return gplidCounter
}

type PrometheusLabelIDsCounter struct {
	ReceiveLabel  uint64 `statsd:"receive_label_count"`
	ReceiveTarget uint64 `statsd:"receive_target_count"`
	SendLabelID   uint64 `statsd:"send_label_id_count"`
	SendTargetID  uint64 `statsd:"send_target_id_count"`
}

func (c *PrometheusLabelIDsCounter) AddReceiveCount(lc, tc uint64) {
	atomic.AddUint64(&c.ReceiveLabel, lc)
	atomic.AddUint64(&c.ReceiveTarget, tc)
}

func (c *PrometheusLabelIDsCounter) AddSendCount(lc, tc uint64) {
	atomic.AddUint64(&c.SendLabelID, lc)
	atomic.AddUint64(&c.SendTargetID, tc)
}

type GetPrometheusLabelIDsCounter struct {
	*PrometheusLabelIDsCounter
}

func NewGetPrometheusLabelIDsCounter() *GetPrometheusLabelIDsCounter {
	return &GetPrometheusLabelIDsCounter{
		PrometheusLabelIDsCounter: &PrometheusLabelIDsCounter{},
	}
}

func (g *GetPrometheusLabelIDsCounter) GetCounter() interface{} {
	counter := &PrometheusLabelIDsCounter{}
	counter, g.PrometheusLabelIDsCounter = g.PrometheusLabelIDsCounter, counter
	return counter
}

func (g *GetPrometheusLabelIDsCounter) Closed() bool {
	return false
}
