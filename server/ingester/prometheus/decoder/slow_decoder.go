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

package decoder

import (
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	logging "github.com/op/go-logging"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	"github.com/deepflowio/deepflow/message/trident"
	"github.com/deepflowio/deepflow/server/ingester/common"
	"github.com/deepflowio/deepflow/server/ingester/prometheus/config"
	"github.com/deepflowio/deepflow/server/ingester/prometheus/dbwriter"
	"github.com/deepflowio/deepflow/server/libs/grpc"
	"github.com/deepflowio/deepflow/server/libs/pool"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"github.com/deepflowio/deepflow/server/libs/stats"
	"github.com/deepflowio/deepflow/server/libs/utils"
)

type PrometheusNames struct {
	MetricsName string
	Job         string
	Instance    string
	Names       []string
	Values      []string
}

type SlowCounter struct {
	InCount            int64 `statsd:"in-count"`
	OutCount           int64 `statsd:"out-count"`
	ErrMetrics         int64 `statsd:"err-metrics"`
	TimeSeries         int64 `statsd:"time-series"` // only for prometheus, count the number of TimeSeries (not Samples)
	ErrLableId         int64 `statsd:"err-labelid"`
	RequestTimes       int64 `statsd:"request-times"`
	RequestMetricCount int64 `statsd:"request-metric-count"`
	RequestTimeNs      int64 `statsd:"request-time-ns"`
}

type SlowItem struct {
	vtapID uint16
	ts     *prompb.TimeSeries
}

var slowItemPool = pool.NewLockFreePool(func() interface{} {
	return &SlowItem{}
})

func AcquireSlowItem(vtapId uint16, ts *prompb.TimeSeries) *SlowItem {
	s := slowItemPool.Get().(*SlowItem)
	s.vtapID = vtapId
	s.ts = ts
	return s
}

func ReleaseSlowItem(p *SlowItem) {
	p.ts = nil
	slowItemPool.Put(p)
}

type SlowDecoder struct {
	index            int
	inQueue          queue.QueueReader
	debugEnabled     bool
	config           *config.Config
	prometheusWriter *dbwriter.PrometheusWriter

	buildPrometheus *BuildPrometheus

	counter *SlowCounter
	utils.Closable
}

func NewSlowDecoder(
	index int,
	platformData *grpc.PlatformInfoTable,
	prometheusLabelTable *PrometheusLabelTable,
	inQueue queue.QueueReader,
	prometheusWriter *dbwriter.PrometheusWriter,
	config *config.Config,
) *SlowDecoder {
	return &SlowDecoder{
		index:            index,
		buildPrometheus:  &BuildPrometheus{platformData: platformData, labelTable: prometheusLabelTable},
		inQueue:          inQueue,
		debugEnabled:     log.IsEnabledFor(logging.DEBUG),
		prometheusWriter: prometheusWriter,
		config:           config,
		counter:          &SlowCounter{},
	}
}

func (d *SlowDecoder) GetCounter() interface{} {
	var counter *SlowCounter
	counter, d.counter = d.counter, &SlowCounter{}
	return counter
}

func (d *SlowDecoder) Run() {
	common.RegisterCountableForIngester("decoder", d, stats.OptionStatTags{
		"thread":   strconv.Itoa(d.index),
		"msg_type": "slow_prometheus"})

	batchSize := d.config.GrpcMetricBatchCount
	buffer := make([]interface{}, batchSize)
	req := trident.PrometheusLabelIDsRequest{}
	slowItems := make([]*SlowItem, 0, batchSize)
	queueTicker := 0
	for {
		n := d.inQueue.Gets(buffer)
		for i := 0; i < n; i++ {
			if buffer[i] == nil {
				queueTicker++
				continue
			}
			d.counter.InCount++
			slowItem, ok := buffer[i].(*SlowItem)
			if !ok {
				continue
			}
			slowItems = append(slowItems, slowItem)
			r := d.TimeSeriesToLableIDRequest(slowItem.ts)
			req.RequestLabels = append(req.RequestLabels, r)
		}

		if len(slowItems) < batchSize && queueTicker == 0 {
			continue
		}

		startTime := time.Now()
		d.buildPrometheus.labelTable.RequesteLabelIDs(&req)
		d.counter.RequestTimeNs += int64(time.Since(startTime))
		d.counter.RequestTimes++
		d.counter.RequestMetricCount += int64(len(slowItems))

		for _, item := range slowItems {
			d.sendPrometheus(item.vtapID, item.ts)
			ReleaseSlowItem(item)
		}
		req.RequestLabels = req.RequestLabels[:0]
		slowItems = slowItems[:0]
		queueTicker = 0
	}
}

func addLabelRequest(req *trident.MetricLabelRequest, name, value string) {
	req.Labels = append(req.Labels,
		&trident.LabelRequest{
			Name:  proto.String(name),
			Value: proto.String(value),
		})
}

func (d *SlowDecoder) TimeSeriesToLableIDRequest(ts *prompb.TimeSeries) *trident.MetricLabelRequest {
	req := &trident.MetricLabelRequest{}
	for _, l := range ts.Labels {
		if l.Name == model.MetricNameLabel {
			req.MetricName = proto.String(l.Value)
			continue
		}
		if _, ok := d.buildPrometheus.labelTable.QueryNameID(l.Name); !ok {
			addLabelRequest(req, l.Name, l.Value)
		} else if _, ok := d.buildPrometheus.labelTable.QueryNameID(l.Value); !ok {
			addLabelRequest(req, l.Name, l.Value)
		} else if l.Name == model.JobLabel || l.Name == model.InstanceLabel {
			addLabelRequest(req, l.Name, l.Value)
		}
	}
	return req
}

func (d *SlowDecoder) sendPrometheus(vtapID uint16, ts *prompb.TimeSeries) {
	if d.debugEnabled {
		log.Debugf("decoder %d vtap %d recv promtheus timeseries: %v", d.index, vtapID, ts)
	}
	isSlowItem, err := d.buildPrometheus.TimeSeriesToStore(vtapID, ts)
	if err != nil {
		if d.counter.ErrMetrics == 0 {
			log.Warning(err)
		}
		d.counter.ErrMetrics++
		return
	}
	if isSlowItem {
		d.counter.ErrLableId++
		return
	}
	d.prometheusWriter.WriteBatch(d.buildPrometheus.prometheusBuffer, d.buildPrometheus.labelNamesBuffer, d.buildPrometheus.labelValuesBuffer)
	d.counter.OutCount += int64(len(d.buildPrometheus.prometheusBuffer))
	d.counter.TimeSeries++
}
