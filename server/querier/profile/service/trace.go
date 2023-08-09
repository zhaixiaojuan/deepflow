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

package service

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	logging "github.com/op/go-logging"

	controller_common "github.com/deepflowio/deepflow/server/controller/common"
	ingester_common "github.com/deepflowio/deepflow/server/ingester/profile/common"
	"github.com/deepflowio/deepflow/server/querier/config"
	"github.com/deepflowio/deepflow/server/querier/profile/common"
	"github.com/deepflowio/deepflow/server/querier/profile/model"
)

var log = logging.MustGetLogger("profile")

func Tracing(args model.ProfileTracing, cfg *config.QuerierConfig) (result []*model.ProfileTreeNode, debug interface{}, err error) {
	whereSlice := []string{}
	whereSlice = append(whereSlice, fmt.Sprintf(" time>=%d", args.TimeStart))
	whereSlice = append(whereSlice, fmt.Sprintf(" time<=%d", args.TimeEnd))
	whereSlice = append(whereSlice, fmt.Sprintf(" app_service='%s'", args.AppService))
	whereSlice = append(whereSlice, fmt.Sprintf(" profile_language_type='%s'", args.ProfileLanguageType))
	whereSlice = append(whereSlice, fmt.Sprintf(" profile_event_type='%s'", args.ProfileEventType))
	if args.TagFilter != "" {
		whereSlice = append(whereSlice, " "+args.TagFilter)
	}
	whereSql := strings.Join(whereSlice, " AND")
	limitSql := cfg.Profile.FlameQueryLimit
	url := fmt.Sprintf("http://%s/v1/query/?debug=true", net.JoinHostPort("localhost", fmt.Sprintf("%d", cfg.ListenPort)))
	body := map[string]interface{}{}
	body["db"] = common.DATABASE_PROFILE
	sql := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s LIMIT %d",
		common.PROFILE_LOCATION_STR, common.PROFILE_VALUE, common.TABLE_PROFILE, whereSql, limitSql,
	)
	body["sql"] = sql
	profileDebug := model.Debug{}
	profileDebug.Sql = sql
	resp, err := controller_common.CURLPerform("POST", url, body)
	if err != nil {
		log.Errorf("call querier failed: %s, %s", err.Error(), url)
		return
	}
	if len(resp.Get("result").MustMap()) == 0 {
		log.Warningf("no data in curl response: %s", url)
		return
	}
	profileDebug.IP = resp.Get("debug").Get("ip").MustString()
	profileDebug.QueryUUID = resp.Get("debug").Get("query_uuid").MustString()
	profileDebug.SqlCH = resp.Get("debug").Get("sql").MustString()
	profileDebug.Error = resp.Get("debug").Get("error").MustString()
	profileDebug.QueryTime = resp.Get("debug").Get("query_time").MustString()
	formatStartTime := time.Now()
	profileLocationStrIndex := -1
	profileValueIndex := -1
	NodeIDToProfileTree := map[string]*model.ProfileTreeNode{}
	columns := resp.GetPath("result", "columns")
	values := resp.GetPath("result", "values")
	for columnIndex := range columns.MustArray() {
		column := columns.GetIndex(columnIndex).MustString()
		switch column {
		case "profile_location_str":
			profileLocationStrIndex = columnIndex
		case "profile_value":
			profileValueIndex = columnIndex
		}
	}
	indexOK := slices.Contains[int]([]int{profileLocationStrIndex, profileValueIndex}, -1)
	if indexOK {
		log.Error("Not all fields found")
		err = errors.New("Not all fields found")
		return
	}
	// merge profile_node_ids, profile_parent_node_ids, self_value
	for valueIndex := range values.MustArray() {
		profileLocationStr := values.GetIndex(valueIndex).GetIndex(profileLocationStrIndex).MustString()
		dst := make([]byte, 0, len(profileLocationStr))
		profileLocationStrByte, _ := ingester_common.ZstdDecompress(dst, []byte(profileLocationStr))
		profileLocationStrSlice := strings.Split(string(profileLocationStrByte), ";")
		profileValue := values.GetIndex(valueIndex).GetIndex(profileValueIndex).MustInt()
		for profileLocationIndex := range profileLocationStrSlice {
			nodeProfileValue := 0
			if profileLocationIndex == len(profileLocationStrSlice)-1 {
				nodeProfileValue = profileValue
			}
			profileLocationStrs := strings.Join(profileLocationStrSlice[:profileLocationIndex+1], ";")
			nodeID := controller_common.GenerateUUID(profileLocationStrs)
			existNode, ok := NodeIDToProfileTree[nodeID]
			if ok {
				existNode.SelfValue += nodeProfileValue
				existNode.TotalValue = existNode.SelfValue
			} else {
				node := NewProfileTreeNode(profileLocationStrs, nodeID, nodeProfileValue)
				if profileLocationIndex == 0 {
					node.ParentNodeID = ""
				} else {
					parentProfileLocationStrs := strings.Join(profileLocationStrSlice[:profileLocationIndex], ";")
					node.ParentNodeID = controller_common.GenerateUUID(parentProfileLocationStrs)
				}
				NodeIDToProfileTree[nodeID] = node
			}
		}
	}

	rootTotalValue := 0
	// update total_value
	for _, node := range NodeIDToProfileTree {
		if node.SelfValue == 0 {
			continue
		}
		rootTotalValue += node.SelfValue
		parentNode, ok := NodeIDToProfileTree[node.ParentNodeID]
		if ok {
			UpdateNodeTotalValue(node, parentNode, NodeIDToProfileTree)
		}
	}
	// format root node
	rootNode := NewProfileTreeNode("", "", 0)
	rootNode.ParentNodeID = "-1"
	rootNode.TotalValue = rootTotalValue

	result = append(result, rootNode)
	for _, node := range NodeIDToProfileTree {
		result = append(result, node)
	}
	formatEndTime := int64(time.Since(formatStartTime))
	formatTime := fmt.Sprintf("%.9fs", float64(formatEndTime)/1e9)
	profileDebug.FormatTime = formatTime
	debug = profileDebug
	return
}

func NewProfileTreeNode(profileLocationStrs string, nodeID string, profileValue int) *model.ProfileTreeNode {
	node := &model.ProfileTreeNode{}
	node.ProfileLocationStrs = profileLocationStrs
	node.NodeID = nodeID
	node.SelfValue = profileValue
	node.TotalValue = profileValue
	return node
}

func UpdateNodeTotalValue(node *model.ProfileTreeNode, parentNode *model.ProfileTreeNode, NodeIDToProfileTree map[string]*model.ProfileTreeNode) {
	parentNode.TotalValue += node.SelfValue
	if parentNode.ParentNodeID == "" {
		return
	}
	newParentNode, ok := NodeIDToProfileTree[parentNode.ParentNodeID]
	if ok {
		UpdateNodeTotalValue(parentNode, newParentNode, NodeIDToProfileTree)
	}
}
