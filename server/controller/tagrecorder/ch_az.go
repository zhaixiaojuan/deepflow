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

package tagrecorder

import (
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
	"github.com/deepflowio/deepflow/server/controller/recorder/pubsub/message"
)

type ChAZ struct {
	// UpdaterComponent[mysql.ChAZ, IDKey]
	SubscriberComponent[*message.AZFieldsUpdate, message.AZFieldsUpdate, mysql.AZ, mysql.ChAZ, IDKey]
	domainLcuuidToIconID map[string]int
	resourceTypeToIconID map[IconKey]int
}

func NewChAZ(domainLcuuidToIconID map[string]int, resourceTypeToIconID map[IconKey]int) *ChAZ {
	updater := &ChAZ{
		// newUpdaterComponent[mysql.ChAZ, IDKey](
		// 	RESOURCE_TYPE_CH_AZ,
		// ),
		newSubscriberComponent[*message.AZFieldsUpdate, message.AZFieldsUpdate, mysql.AZ, mysql.ChAZ, IDKey](
			common.RESOURCE_TYPE_AZ_EN,
		),
		domainLcuuidToIconID,
		resourceTypeToIconID,
	}
	// updater.updaterDG = updater
	updater.subscriberDG = updater
	return updater
}

func (a *ChAZ) onResourceUpdated(sourceID int, fieldsUpdate *message.AZFieldsUpdate) {
	updateInfo := make(map[string]interface{})
	if fieldsUpdate.Name.IsDifferent() {
		updateInfo["name"] = fieldsUpdate.Name.GetNew()
	}
	// if oldItem.IconID != newItem.IconID { // TODO need icon id
	// 	updateInfo["icon_id"] = newItem.IconID
	// }
	// TODO refresh control
	if len(updateInfo) > 0 {
		var chItem mysql.ChAZ
		mysql.Db.Where("id = ?", sourceID).First(&chItem)
		a.SubscriberComponent.dbOperator.update(chItem, updateInfo, IDKey{ID: sourceID})
	}
}

func (a *ChAZ) sourceToTarget(az *mysql.AZ) (keys []IDKey, targets []mysql.ChAZ) {
	iconID := a.domainLcuuidToIconID[az.Domain]
	if iconID == 0 {
		key := IconKey{
			NodeType: RESOURCE_TYPE_AZ,
		}
		iconID = a.resourceTypeToIconID[key]
	}
	keys = append(keys, IDKey{ID: az.ID})
	name := az.Name
	if az.DeletedAt.Valid {
		name += " (deleted)"
	}
	targets = append(targets, mysql.ChAZ{
		ID:     az.ID,
		Name:   name,
		IconID: iconID,
	})
	return
}

// // generateNewData implements interface updaterDataGenerator
// func (a *ChAZ) generateNewData() (map[IDKey]mysql.ChAZ, bool) {
// 	log.Infof("generate data for %s", a.UpdaterComponent.resourceTypeName)
// 	var azs []*mysql.AZ
// 	err := mysql.Db.Unscoped().Find(&azs).Error
// 	if err != nil {
// 		log.Errorf(dbQueryResourceFailed(a.UpdaterComponent.resourceTypeName, err))
// 		return nil, false
// 	}
// 	return a.generateKeyToTarget(azs), true
// }

// // TODO move to updater
// func (a *ChAZ) generateKeyToTarget(sources []*mysql.AZ) map[IDKey]mysql.ChAZ {
// 	keyToItem := make(map[IDKey]mysql.ChAZ)
// 	for _, az := range sources {
// 		ks, ts := a.sourceToTarget(az)
// 		for i, k := range ks {
// 			keyToItem[k] = ts[i]
// 		}
// 	}
// 	return keyToItem
// }

// // generateKey implements interface updaterDataGenerator
// func (a *ChAZ) generateKey(dbItem mysql.ChAZ) IDKey { // TODO modify func name
// 	return IDKey{ID: dbItem.ID}
// }

// // generateUpdateInfo implements interface updaterDataGenerator
// func (a *ChAZ) generateUpdateInfo(oldItem, newItem mysql.ChAZ) (map[string]interface{}, bool) {
// 	updateInfo := make(map[string]interface{})
// 	if oldItem.Name != newItem.Name {
// 		updateInfo["name"] = newItem.Name
// 	}
// 	if oldItem.IconID != newItem.IconID {
// 		updateInfo["icon_id"] = newItem.IconID
// 	}
// 	if len(updateInfo) > 0 {
// 		return updateInfo, true
// 	}
// 	return nil, false
// }
