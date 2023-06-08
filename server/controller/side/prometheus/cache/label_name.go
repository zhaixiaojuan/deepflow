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
	"sync"

	"github.com/deepflowio/deepflow/message/controller"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
)

type labelName struct {
	nameToID sync.Map
}

func (t *labelName) GetIDByName(n string) (int, bool) {
	if id, ok := t.nameToID.Load(n); ok {
		return id.(int), true
	}
	return 0, false
}

func (t *labelName) Add(batch []*controller.PrometheusLabelName) {
	for _, m := range batch {
		t.nameToID.Store(m.GetName(), int(m.GetId()))
	}
}

func (t *labelName) refresh(args ...interface{}) error {
	labelNames, err := t.load()
	if err != nil {
		return err
	}
	for _, ln := range labelNames {
		t.nameToID.Store(ln.Name, ln.ID)
	}
	return nil
}

func (t *labelName) load() ([]*mysql.PrometheusLabelName, error) {
	var labelNames []*mysql.PrometheusLabelName
	err := mysql.Db.Find(&labelNames).Error
	return labelNames, err
}