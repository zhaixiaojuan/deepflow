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
	"fmt"
	"net"
	"strings"

	"golang.org/x/net/context"

	"github.com/cornelk/hashmap"
	"github.com/deepflowio/deepflow/message/trident"
	"github.com/deepflowio/deepflow/server/libs/debug"
	"github.com/deepflowio/deepflow/server/libs/grpc"
	//"github.com/gogo/protobuf/proto"
	"github.com/pyroscope-io/pyroscope/pkg/scrape/model"
)

const (
	CMD_PROMETHEUS_LABEL = 38
	METRICID_OFFSET      = 42 // metricID max 1<<22
	JOBID_OFFSET         = 21 // jobID/instanceID max 1<<21
)

func (p *PrometheusLabelTable) QueryMetricID(name string) (uint32, bool) {
	return p.metricNameIDs.Get(name)
}

func (p *PrometheusLabelTable) QueryNameID(name string) (uint32, bool) {
	return p.labelNameIDs.Get(name)
}

func (p *PrometheusLabelTable) QueryValueID(value string) (uint32, bool) {
	return p.labelValueIDs.Get(value)
}

func (p *PrometheusLabelTable) QueryColumnIndex(metricID, nameID uint32) (uint32, bool) {
	return p.labelColumnIndexs.Get(uint64(metricID)<<METRICID_OFFSET | uint64(nameID))
}

func (p *PrometheusLabelTable) QueryTargetID(metricID, jobID, instanceID uint32) (uint32, bool) {
	return p.labelColumnIndexs.Get(uint64(metricID)<<METRICID_OFFSET | uint64(jobID)<<JOBID_OFFSET | uint64(instanceID))
}

type PrometheusLabelTable struct {
	ctlIP               string
	GrpcSession         *grpc.GrpcSession
	versionPlatformData uint64

	metricNameIDs     *hashmap.Map[string, uint32]
	labelNameIDs      *hashmap.Map[string, uint32]
	labelValueIDs     *hashmap.Map[string, uint32]
	labelColumnIndexs *hashmap.Map[uint64, uint32]
	targetIDs         *hashmap.Map[uint64, uint32]
}

func NewPrometheusLabelTable(controllerIPs []string, port, rpcMaxMsgSize int) *PrometheusLabelTable {
	ips := make([]net.IP, len(controllerIPs))
	for i, ipString := range controllerIPs {
		ips[i] = net.ParseIP(ipString)
		if ips[i].To4() != nil {
			ips[i] = ips[i].To4()
		}
	}
	p := &PrometheusLabelTable{
		GrpcSession:       &grpc.GrpcSession{},
		metricNameIDs:     hashmap.New[string, uint32](),
		labelNameIDs:      hashmap.New[string, uint32](),
		labelValueIDs:     hashmap.New[string, uint32](),
		labelColumnIndexs: hashmap.New[uint64, uint32](),
		targetIDs:         hashmap.New[uint64, uint32](),
	}
	p.GrpcSession.Init(ips, uint16(port), grpc.DEFAULT_SYNC_INTERVAL, rpcMaxMsgSize, nil)
	log.Infof("New PrometheusLabelTable ips:%v port:%d rpcMaxMsgSize:%d", ips, port, rpcMaxMsgSize)
	debug.ServerRegisterSimple(CMD_PROMETHEUS_LABEL, p)
	return p
}

var metricCounter uint32
var nameCounter uint32
var valueCounter uint32
var targetCounter uint32
var columnCounter uint32

func (p *PrometheusLabelTable) RequesteLabelIDs(request *trident.PrometheusLabelIDsRequest) error {
	var response *trident.PrometheusLabelIDsResponse
	err := p.GrpcSession.Request(func(ctx context.Context, remote net.IP) error {
		var err error
		c := p.GrpcSession.GetClient()
		if c == nil {
			return fmt.Errorf("can't get grpc client to %s", remote)
		}
		client := trident.NewSynchronizerClient(c)
		response, err = client.GetPrometheusLabelIDs(ctx, request)
		return err
	})
	if err != nil {
		return err
	}
	/*
		response = &trident.PrometheusLabelIDsResponse{}

		for _, r := range request.RequestLabels {
			LResps := []*trident.LabelIDResponse{}
			for _, l := range r.Labels {
				nameCounter++
				valueCounter++
				columnCounter++
				LResps = append(LResps,
					&trident.LabelIDResponse{
						Name:                l.Name,
						Value:               l.Value,
						NameId:              proto.Uint32(nameCounter),
						ValueId:             proto.Uint32(valueCounter),
						AppLabelColumnIndex: proto.Uint32(columnCounter % 23)})
			}

			metricCounter++
			targetCounter++
			m := &trident.MetricLabelResponse{
				MetricName: r.MetricName,
				MetricId:   proto.Uint32(metricCounter),
				TargetId:   proto.Uint32(targetCounter),
				LabelIds:   LResps,
			}
			response.ResponseLabelIds = append(response.ResponseLabelIds, m)
		}
	*/

	log.Infof("response ====: %s", response)
	p.updatePrometheusLabels(response)

	return nil
}

func (p *PrometheusLabelTable) RequesteAllLabelIDs() {
	err := p.RequesteLabelIDs(&trident.PrometheusLabelIDsRequest{})
	if err != nil {
		log.Warning("request all prometheus label ids failed: %s", err)
	}
}

func (p *PrometheusLabelTable) updatePrometheusLabels(resp *trident.PrometheusLabelIDsResponse) {
	for _, metric := range resp.GetResponseLabelIds() {
		metricName := metric.GetMetricName()
		if metricName == "" {
			continue
		}
		targetId := metric.GetTargetId()
		if targetId == 0 {
			//	continue
		}
		metricId := metric.GetMetricId()
		if metricId == 0 {
			continue
		}
		p.metricNameIDs.Set(metricName, metricId)
		var jobId, instanceId uint32
		for _, labelInfo := range metric.GetLabelIds() {
			name := labelInfo.GetName()
			nameId := labelInfo.GetNameId()
			if name != "" && nameId != 0 {
				p.labelNameIDs.Set(name, nameId)
			}
			value := labelInfo.GetValue()
			valueId := labelInfo.GetValueId()
			if value != "" && valueId != 0 {
				p.labelValueIDs.Set(value, valueId)
			}
			if name == model.JobLabel {
				jobId = valueId
			} else if name == model.InstanceLabel {
				instanceId = valueId
			}
			cIndex := labelInfo.GetAppLabelColumnIndex()
			if cIndex == 0 {
				continue
			}
			p.labelColumnIndexs.Set(uint64(metricId)<<METRICID_OFFSET|uint64(nameId), cIndex)
		}
		if jobId > 0 || instanceId > 0 {
			// p.targetIDs.Set(uint64(metricId)<<METRICID_OFFSET|uint64(jobId)<<JOBID_OFFSET|uint64(instanceId), targetId)
			p.targetIDs.Set(uint64(metricId)<<METRICID_OFFSET|uint64(jobId)<<JOBID_OFFSET|uint64(instanceId), uint32(metricId)<<22|uint32(jobId)<<22>>12|instanceId)
		}
	}
}

func (p *PrometheusLabelTable) metricIDsString() string {
	sb := &strings.Builder{}
	sb.WriteString("\nmetricName                                                                                    metricId\n")
	sb.WriteString("--------------------------------------------------------------------------------------------------------\n")
	p.metricNameIDs.Range(func(k string, v uint32) bool {
		sb.WriteString(fmt.Sprintf("%-100s  %d\n", k, v))
		return true
	})
	return sb.String()
}

func (p *PrometheusLabelTable) nameIDsString() string {
	sb := &strings.Builder{}
	sb.WriteString("\nname                                      nameId\n")
	sb.WriteString("--------------------------------------------------\n")
	p.labelNameIDs.Range(func(k string, v uint32) bool {
		sb.WriteString(fmt.Sprintf("%-32s  %d\n", k, v))
		return true
	})
	return sb.String()
}

func (p *PrometheusLabelTable) valueIDsString() string {
	sb := &strings.Builder{}
	sb.WriteString("\nvalue                                                                                                                                 valueId\n")
	sb.WriteString("-----------------------------------------------------------------------------------------------------------------------------------------------\n")
	p.labelValueIDs.Range(func(k string, v uint32) bool {
		sb.WriteString(fmt.Sprintf("%-128s  %d\n", k, v))
		return true
	})
	return sb.String()
}

func (p *PrometheusLabelTable) columnIndexString() string {
	sb := &strings.Builder{}
	sb.WriteString("\ncolumnIndex  metricName                                                                                           metricId  name                             nameId\n")
	sb.WriteString("----------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")
	p.labelColumnIndexs.Range(func(k uint64, v uint32) bool {
		metricId := k >> METRICID_OFFSET
		nameId := k << (64 - METRICID_OFFSET) >> (64 - METRICID_OFFSET)
		metricName, name := "", ""
		p.metricNameIDs.Range(func(n string, i uint32) bool {
			if i == uint32(metricId) {
				metricName = n
				return false
			}
			return true
		})
		p.labelNameIDs.Range(func(n string, i uint32) bool {
			if i == uint32(nameId) {
				name = n
				return false
			}
			return true
		})
		sb.WriteString(fmt.Sprintf("%-11d  %-100s  %-9d  %-32s  %-6d\n", v, metricName, metricId, name, nameId))
		return true
	})
	return sb.String()
}

func (p *PrometheusLabelTable) targetString() string {
	sb := &strings.Builder{}
	sb.WriteString("\ntargetId metricName                                                                                        metricId  job                                                           jobId  instance                           instanceId\n")
	sb.WriteString("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")
	p.targetIDs.Range(func(k uint64, v uint32) bool {
		metricId := k >> METRICID_OFFSET
		jobId := k << (64 - METRICID_OFFSET) >> (JOBID_OFFSET + METRICID_OFFSET)
		instanceId := k << (64 - JOBID_OFFSET) >> (64 - JOBID_OFFSET)
		metricName, job, instance := "", "", ""
		p.metricNameIDs.Range(func(n string, i uint32) bool {
			if i == uint32(metricId) {
				metricName = n
				return false
			}
			return true
		})
		p.labelValueIDs.Range(func(n string, i uint32) bool {
			if i == uint32(jobId) {
				job = n
			} else if i == uint32(instanceId) {
				instance = n
			}
			if job != "" && instance != "" {
				return false
			}
			return true
		})
		sb.WriteString(fmt.Sprintf("%-8d   %-100s  %-8d  %-64s  %-5d   %-32s     %d\n", v, metricName, metricId, job, jobId, instance, instanceId))
		return true

	})
	return sb.String()
}

func getStringMapMaxValue(m *hashmap.Map[string, uint32]) uint32 {
	maxId := uint32(0)
	m.Range(func(n string, i uint32) bool {
		if i > maxId {
			maxId = i
		}
		return true
	})
	return maxId
}

func getUInt64MapMaxValue(m *hashmap.Map[uint64, uint32]) uint32 {
	maxId := uint32(0)
	m.Range(func(n uint64, i uint32) bool {
		if i > maxId {
			maxId = i
		}
		return true
	})
	return maxId
}

func (p *PrometheusLabelTable) totalStatsString() string {
	sb := &strings.Builder{}
	sb.WriteString("\ntableType  total-count  max-id\n")
	sb.WriteString("--------------------------------------------\n")
	sb.WriteString(fmt.Sprintf("%-9s  %-11d  %-6d\n", "metric", p.metricNameIDs.Len(), getStringMapMaxValue(p.metricNameIDs)))
	sb.WriteString(fmt.Sprintf("%-9s  %-11d  %-6d\n", "name", p.labelNameIDs.Len(), getStringMapMaxValue(p.labelNameIDs)))
	sb.WriteString(fmt.Sprintf("%-9s  %-11d  %-6d\n", "value", p.labelValueIDs.Len(), getStringMapMaxValue(p.labelValueIDs)))
	sb.WriteString(fmt.Sprintf("%-9s  %-11d  %-6d\n", "column", p.labelColumnIndexs.Len(), getUInt64MapMaxValue(p.labelColumnIndexs)))
	sb.WriteString(fmt.Sprintf("%-9s  %-11d  %-6d\n", "target", p.targetIDs.Len(), getUInt64MapMaxValue(p.targetIDs)))
	return sb.String()
}

func (p *PrometheusLabelTable) HandleSimpleCommand(op uint16, arg string) string {
	switch arg {
	case "metric":
		return p.metricIDsString()
	case "name":
		return p.nameIDsString()
	case "value":
		return p.valueIDsString()
	case "column":
		return p.columnIndexString()
	case "target":
		return p.targetString()
	}
	return p.totalStatsString()
}
