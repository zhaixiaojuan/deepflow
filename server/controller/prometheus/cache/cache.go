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

package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/op/go-logging"
	"golang.org/x/sync/errgroup"

	"github.com/deepflowio/deepflow/message/controller"
	. "github.com/deepflowio/deepflow/server/controller/prometheus/common"
)

var log = logging.MustGetLogger("prometheus")

var (
	cacheOnce sync.Once
	cacheIns  *Cache
)

type Cache struct {
	ctx context.Context

	canRefresh chan bool

	MetricName              *metricName
	LabelName               *labelName
	LabelValue              *labelValue
	MetricAndAPPLabelLayout *metricAndAPPLabelLayout
	Target                  *target
	Label                   *label
	MetricLabel             *metricLabel
	MetricTarget            *metricTarget
}

func GetSingleton() *Cache {
	cacheOnce.Do(func() {
		l := newLabel()
		cacheIns = &Cache{
			canRefresh:              make(chan bool, 1),
			MetricName:              &metricName{},
			LabelName:               &labelName{},
			LabelValue:              &labelValue{},
			MetricAndAPPLabelLayout: &metricAndAPPLabelLayout{},
			Target:                  newTarget(),
			Label:                   l,
			MetricLabel:             newMetricLabel(l),
			MetricTarget:            newMetricTarget(),
		}
	})
	return cacheIns
}

func GetDebugCache(t controller.PrometheusCacheType) []byte {
	tempCache := GetSingleton()
	content := make(map[string]interface{})

	getMetricName := func() {
		temp := map[string]interface{}{
			"name_to_id": make(map[string]interface{}),
		}
		tempCache.MetricName.nameToID.Range(func(key, value any) bool {
			temp["name_to_id"].(map[string]interface{})[key.(string)] = value
			return true
		})
		if len(temp["name_to_id"].(map[string]interface{})) > 0 {
			content["metric_name"] = temp
		}
	}
	getLabelName := func() {
		temp := map[string]interface{}{
			"name_to_id": make(map[string]interface{}),
		}
		tempCache.LabelName.nameToID.Range(func(key, value any) bool {
			temp["name_to_id"].(map[string]interface{})[key.(string)] = value
			return true
		})
		if len(temp["name_to_id"].(map[string]interface{})) > 0 {
			content["label_name"] = temp
		}
	}
	getLabelValue := func() {
		temp := map[string]interface{}{
			"value_to_id": make(map[string]interface{}),
		}
		tempCache.LabelValue.valueToID.Range(func(key, value any) bool {
			temp["value_to_id"].(map[string]interface{})[key.(string)] = value
			return true
		})
		if len(temp["value_to_id"].(map[string]interface{})) > 0 {
			content["label_value"] = temp
		}
	}
	getMetricAndAppLabelLayout := func() {
		temp := map[string]interface{}{
			"layout_key_to_index":                    make(map[LayoutKey]interface{}),
			"metric_name_to_app_label_name_to_value": make(map[string]map[string]string),
		}
		tempCache.MetricAndAPPLabelLayout.layoutKeyToIndex.Range(func(key, value any) bool {
			temp["layout_key_to_index"].(map[LayoutKey]interface{})[key.(LayoutKey)] = value
			return true
		})
		for metricName, appLabelNameToValue := range tempCache.MetricAndAPPLabelLayout.metricNameToAPPLabelNameToValue {
			temp["metric_name_to_app_label_name_to_value"].(map[string]interface{})[metricName] = appLabelNameToValue
		}
		if len(temp["layout_key_to_index"].(map[string]interface{})) > 0 ||
			len(temp["metric_name_to_app_label_name_to_value"].(map[string]interface{})) > 0 {
			content["metric_and_app_label_layout"] = temp
		}
	}
	getTarget := func() {
		temp := map[string]interface{}{
			"key_to_target_id":                 make(map[string]interface{}),
			"target_id_to_label_name_to_value": make(map[int]map[string]string),
		}
		tempCache.Target.keyToTargetID.Range(func(key, value any) bool {
			t := key.(TargetKey)
			k, _ := json.Marshal(t)
			temp["key_to_target_id"].(map[string]interface{})[string(k)] = value
			return true
		})
		tempCache.Target.targetIDToLabelNameToValue.Range(func(key, value any) bool {
			temp["target_id_to_label_name_to_value"].(map[int]map[string]string)[key.(int)] = value.(map[string]string)
			return true
		})
		if len(temp["key_to_target_id"].(map[string]interface{})) > 0 ||
			len(temp["target_id_to_label_name_to_value"].(map[int]map[string]string)) > 0 {
			content["target"] = temp
		}
	}
	getLabel := func() {
		temp := map[string]interface{}{
			"key_map":   make(map[LabelKey]interface{}),
			"id_to_key": make(map[string]LabelKey),
		}

		tempCache.Label.idToKey.Range(func(key, value any) bool {
			temp["id_to_key"].(map[int]LabelKey)[key.(int)] = value.(LabelKey)
			return true
		})

		tempCache.Label.keyMap.Range(func(key, value any) bool {
			temp["key_map"].(map[LabelKey]interface{})[key.(LabelKey)] = struct{}{}
			return true
		})
		if len(temp["key_map"].(map[string]interface{})) > 0 ||
			len(temp["id_to_key"].(map[string]interface{})) > 0 {
			content["label"] = temp
		}
	}
	getMetricLabel := func() {
		temp := map[string]interface{}{
			"label_cache": map[string]interface{}{
				"key_map":   make(map[LabelKey]interface{}),
				"id_to_key": make(map[int]LabelKey),
			},
			"metric_name_to_label_ids":    make(map[string][]int),
			"metric_label_detail_key_map": make(map[MetricLabelDetailKey]interface{}),
		}
		tempCache.MetricLabel.LabelCache.keyMap.Range(func(key, value any) bool {
			temp["label_cache"].(map[string]interface{})["key_map"].(map[LabelKey]interface{})[key.(LabelKey)] = value
			return true
		})
		tempCache.MetricLabel.LabelCache.idToKey.Range(func(key, value any) bool {
			temp["label_cache"].(map[string]interface{})["id_to_key"].(map[int]LabelKey)[key.(int)] = value.(LabelKey)
			return true
		})
		for k, v := range tempCache.MetricLabel.metricNameToLabelIDs {
			temp["metric_name_to_label_ids"].(map[string][]int)[k] = v
		}
		tempCache.MetricLabel.metricLabelDetailKeyMap.Range(func(key, value any) bool {
			temp["metric_label_detail_key_map"].(map[MetricLabelDetailKey]interface{})[key.(MetricLabelDetailKey)] = value
			return true
		})
		if len(temp["label_cache"].(map[string]interface{})["key_map"].(map[LabelKey]interface{})) > 0 ||
			len(temp["label_cache"].(map[string]interface{})["id_to_key"].(map[int]LabelKey)) > 0 ||
			len(temp["metric_name_to_label_ids"].(map[string][]int)) > 0 ||
			len(temp["metric_label_detail_key_map"].(map[MetricLabelDetailKey]interface{})) > 0 {
			content["metric_label"] = temp
		}
	}
	getMetricTarget := func() {
		temp := map[string]interface{}{
			"metric_target_key_map": make(map[MetricTargetKey]interface{}),
		}
		tempCache.MetricTarget.metricTargetKeyMap.Range(func(key, value any) bool {
			temp["metric_target_key_map"].(map[MetricTargetKey]interface{})[key.(MetricTargetKey)] = struct{}{}
			return true
		})
		if len(temp["metric_name_to_target_id"].(map[string]interface{})) > 0 {
			content["metric_target"] = temp
		}
	}

	switch t {
	case controller.PrometheusCacheType_ALL:
		getMetricName()
		getLabelName()
		getLabelValue()
		getMetricAndAppLabelLayout()
		getTarget()
		getLabel()
		getMetricLabel()
		getMetricTarget()
	case controller.PrometheusCacheType_METRIC_NAME:
		getMetricName()
	case controller.PrometheusCacheType_LABEL_NAME:
		getLabelName()
	case controller.PrometheusCacheType_LABEL_VALUE:
		getLabelValue()
	case controller.PrometheusCacheType_METRIC_AND_APP_LABEL_LAYOUT:
		getMetricAndAppLabelLayout()
	case controller.PrometheusCacheType_TARGET:
		getTarget()
	case controller.PrometheusCacheType_LABEL:
		getLabel()
	case controller.PrometheusCacheType_METRIC_LABEL:
		getMetricLabel()
	case controller.PrometheusCacheType_METRIC_TARGET:
		getMetricTarget()
	default:
		return nil
	}

	b, _ := json.MarshalIndent(content, "", "	")
	return b
}

func (t *Cache) Start(ctx context.Context) error {
	if err := t.refresh(false); err != nil {
		return err
	}
	t.canRefresh <- true
	go func() {
		ticker := time.NewTicker(time.Hour)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case t.canRefresh <- true:
					t.refresh(false)
				default:
					log.Info("last refresh cache not completed now")
				}
			}
		}
	}()
	return nil
}

func (t *Cache) refresh(fully bool) error {
	log.Info("refresh cache started")
	t.Label.refresh(fully)
	eg := &errgroup.Group{}
	AppendErrGroup(eg, t.MetricName.refresh, fully)
	AppendErrGroup(eg, t.LabelName.refresh, fully)
	AppendErrGroup(eg, t.LabelValue.refresh, fully)
	AppendErrGroup(eg, t.MetricAndAPPLabelLayout.refresh, fully)
	AppendErrGroup(eg, t.MetricLabel.refresh, fully)
	AppendErrGroup(eg, t.Target.refresh, fully)
	AppendErrGroup(eg, t.MetricTarget.refresh, fully)
	err := eg.Wait()
	log.Info("refresh cache completed")
	return err

}

func (t *Cache) RefreshFully() error {
	t.Clear()
	err := t.refresh(true)
	return err
}

func (t *Cache) Clear() {
	t.MetricLabel.clear()
}
