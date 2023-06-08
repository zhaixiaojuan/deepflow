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

package synchronizer

import (
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/deepflowio/deepflow/message/controller"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
)

type labelLayout struct {
	mux                          sync.Mutex
	resourceType                 string
	metricNameToLabelNameToIndex map[string]map[string]uint8
	metricNameToMaxIndex         map[string]uint8
}

func newLabelLayout() *labelLayout {
	return &labelLayout{
		resourceType:                 "metric_app_label_layout",
		metricNameToLabelNameToIndex: make(map[string]map[string]uint8),
		metricNameToMaxIndex:         make(map[string]uint8),
	}
}

func (m *labelLayout) refresh(args ...interface{}) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	var layouts []*mysql.PrometheusMetricAPPLabelLayout
	err := mysql.Db.Find(&layouts).Error
	if err != nil {
		return err
	}

	for _, item := range layouts {
		if _, ok := m.metricNameToLabelNameToIndex[item.MetricName]; !ok {
			m.metricNameToLabelNameToIndex[item.MetricName] = make(map[string]uint8)
		}
		m.metricNameToLabelNameToIndex[item.MetricName][item.APPLabelName] = item.APPLabelColumnIndex
		if m.metricNameToMaxIndex[item.MetricName] < item.APPLabelColumnIndex {
			m.metricNameToMaxIndex[item.MetricName] = item.APPLabelColumnIndex
		}
	}
	return nil
}

func (m *labelLayout) sync(req []*controller.PrometheusMetricAPPLabelLayoutRequest) ([]*controller.PrometheusMetricAPPLabelLayout, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	resp := make([]*controller.PrometheusMetricAPPLabelLayout, 0, len(req))

	tmpMetricNameToMaxIndex := make(map[string]uint8)
	for k, v := range m.metricNameToMaxIndex {
		tmpMetricNameToMaxIndex[k] = v
	}
	var dbToAdd []*mysql.PrometheusMetricAPPLabelLayout
	for _, v := range req {
		mn := v.GetMetricName()
		ln := v.GetAppLabelName()
		lv := v.GetAppLabelValue()
		if _, ok := m.metricNameToLabelNameToIndex[mn][ln]; ok {
			resp = append(resp, &controller.PrometheusMetricAPPLabelLayout{
				MetricName:          &mn,
				AppLabelName:        &ln,
				AppLabelValue:       &lv,
				AppLabelColumnIndex: proto.Uint32(uint32(m.metricNameToLabelNameToIndex[mn][ln])),
			})
		}
		dbToAdd = append(dbToAdd, &mysql.PrometheusMetricAPPLabelLayout{
			MetricName:          mn,
			APPLabelName:        ln,
			APPLabelValue:       v.GetAppLabelValue(),
			APPLabelColumnIndex: tmpMetricNameToMaxIndex[mn] + 1,
		})
		tmpMetricNameToMaxIndex[mn]++
	}
	err := m.addBatch(dbToAdd)
	if err != nil {
		log.Errorf("add %s error: %s", m.resourceType, err.Error())
		return nil, err
	}
	for _, l := range dbToAdd {
		resp = append(resp, &controller.PrometheusMetricAPPLabelLayout{
			MetricName:          &l.MetricName,
			AppLabelName:        &l.APPLabelName,
			AppLabelValue:       &l.APPLabelValue,
			AppLabelColumnIndex: proto.Uint32(uint32(l.APPLabelColumnIndex)),
		})
		if _, ok := m.metricNameToLabelNameToIndex[l.MetricName]; !ok {
			m.metricNameToLabelNameToIndex[l.MetricName] = make(map[string]uint8)
		}
		m.metricNameToLabelNameToIndex[l.MetricName][l.APPLabelName] = l.APPLabelColumnIndex
		if m.metricNameToMaxIndex[l.MetricName] < l.APPLabelColumnIndex {
			m.metricNameToMaxIndex[l.MetricName] = l.APPLabelColumnIndex
		}
	}
	return resp, nil
}

func (m *labelLayout) addBatch(toAdd []*mysql.PrometheusMetricAPPLabelLayout) error {
	count := len(toAdd)
	offset := 1000
	pages := count/offset + 1
	if count%offset == 0 {
		pages = count / offset
	}
	for i := 0; i < pages; i++ {
		start := i * offset
		end := (i + 1) * offset
		if end > count {
			end = count
		}
		oneP := toAdd[start:end]
		err := mysql.Db.Create(&oneP).Error
		if err != nil {
			return err
		}
		log.Infof("add %d %s success", len(oneP), m.resourceType)
	}
	return nil
}