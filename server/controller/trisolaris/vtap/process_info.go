/*
 * Copyright (c) 2022 Yunshan Networks
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

package vtap

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/deepflowio/deepflow/message/trident"
	. "github.com/deepflowio/deepflow/server/controller/common"
	models "github.com/deepflowio/deepflow/server/controller/db/mysql"
	"github.com/deepflowio/deepflow/server/controller/trisolaris/config"
	"github.com/deepflowio/deepflow/server/controller/trisolaris/dbmgr"
	"github.com/deepflowio/deepflow/server/controller/trisolaris/utils"
	libu "github.com/deepflowio/deepflow/server/libs/utils"
)

const (
	GPID0_MASK = 0xFFFFFFFF00000000
	GPID1_MASK = 0x00000000FFFFFFFF
	CACHE_SIZE = 65535

	TCP_PROTO_STR = "TCP"
	UDP_PROTO_STR = "UDP"
)

type GlobalEntry struct {
	vtapId0 uint32
	pid0    uint32
	vtapId1 uint32
	pid1    uint32
}

func newGlobalEntry() *GlobalEntry {
	return &GlobalEntry{}
}

func (g *GlobalEntry) setPid0(pid0 uint32, vtapId0 uint32) {
	if g == nil {
		return
	}
	g.vtapId0 = vtapId0
	g.pid0 = pid0
}

func (g *GlobalEntry) setPid1(pid1 uint32, vtapId1 uint32) {
	if g == nil {
		return
	}
	g.vtapId1 = vtapId1
	g.pid1 = pid1
}

func (g *GlobalEntry) getPid0Data() (pid0, vtapId0 uint32) {
	if g == nil {
		return
	}
	pid0 = g.pid0
	vtapId0 = g.vtapId0
	return
}

func (g *GlobalEntry) getPid1Data() (pid1, vtapId1 uint32) {
	if g == nil {
		return
	}
	pid1 = g.pid1
	vtapId1 = g.vtapId1
	return
}

func (g *GlobalEntry) getData() (pid0, vtapId0, pid1, vtapId1 uint32) {
	if g == nil {
		return
	}
	pid0 = g.pid0
	vtapId0 = g.vtapId0
	pid1 = g.pid1
	vtapId1 = g.vtapId1
	return
}

// epcId(u16),port(u16),ip(u32)
func generateEPKey(epcId, port, ip uint32) uint64 {
	epcIDPort := epcId<<16 | port
	return uint64(epcIDPort)<<32 | uint64(ip)
}

func getEpcIdPortIP(value uint64) (epcId, port, ip uint32) {
	ip = uint32(value & 0xffffffff)
	epcIdPort := uint32(value >> 32)
	port = (epcIdPort & 0xffff)
	epcId = epcIdPort >> 16
	return
}

func convertProto(proto string) trident.ServiceProtocol {
	switch proto {
	case TCP_PROTO_STR:
		return trident.ServiceProtocol_TCP_SERVICE
	case UDP_PROTO_STR:
		return trident.ServiceProtocol_UDP_SERVICE
	}

	return 0
}

var serviceTypes = [MAX_SERVICE_TYPE]int{TCPService, UDPService}

const (
	TCPService = iota
	UDPService
	MAX_SERVICE_TYPE
)

type EntryData [MAX_SERVICE_TYPE]*utils.U128IDMap

func NewEntryData() EntryData {
	var entryData EntryData
	for index, _ := range entryData {
		entryData[index] = utils.NewU128IDMapNoStats("trisolaris-global-gpid", CACHE_SIZE)
	}

	return entryData
}

func (d EntryData) getAggregateMap(protocol trident.ServiceProtocol) *utils.U128IDMap {
	serviceIndex := MAX_SERVICE_TYPE
	switch {
	case protocol == trident.ServiceProtocol_TCP_SERVICE:
		serviceIndex = TCPService
	case protocol == trident.ServiceProtocol_UDP_SERVICE:
		serviceIndex = UDPService
	}
	if serviceIndex == MAX_SERVICE_TYPE {
		return nil
	}

	return d[serviceIndex]
}

func (d EntryData) addData(vtapId uint32, entry *trident.GPIDSyncEntry, p *ProcessInfo) {
	aggregateMap := d.getAggregateMap(entry.GetProtocol())
	if aggregateMap == nil {
		return
	}
	pid0, pid1 := entry.GetPid_0(), entry.GetPid_1()
	if pid0 > 0 || pid1 > 0 {
		key0, key1 := p.getKey(entry)
		value := newGlobalEntry()
		if pid0 > 0 {
			value.setPid0(pid0, vtapId)
		}
		if pid1 > 0 {
			value.setPid1(pid1, vtapId)
		}
		mapValue, add := aggregateMap.AddOrGet(key0, key1, value, false)
		if add == false {
			entry := mapValue.(*GlobalEntry)
			if pid0 > 0 {
				entry.setPid0(pid0, vtapId)
			}
			if pid1 > 0 {
				entry.setPid1(pid1, vtapId)
			}
		}
	}
}

func (d EntryData) getData(entry *trident.GPIDSyncEntry, p *ProcessInfo) *GlobalEntry {
	aggregateMap := d.getAggregateMap(entry.GetProtocol())
	if aggregateMap == nil {
		return nil
	}
	key0, key1 := p.getKey(entry)
	value, _ := aggregateMap.Get(key0, key1)
	if value == nil {
		return nil
	}
	return value.(*GlobalEntry)
}

func (e EntryData) getGPIDGlobalData(p *ProcessInfo) []*trident.GlobalGPIDEntry {

	allData := []*trident.GlobalGPIDEntry{}
	for _, serviceIndex := range serviceTypes {
		var protocol trident.ServiceProtocol
		switch serviceIndex {
		case TCPService:
			protocol = trident.ServiceProtocol_TCP_SERVICE
		case UDPService:
			protocol = trident.ServiceProtocol_UDP_SERVICE
		}
		if serviceIndex >= MAX_SERVICE_TYPE {
			break
		}
		for keyValue := range e[serviceIndex].Iter() {
			key0, key1, value := keyValue.GetData()
			epcId0, port0, ip0 := getEpcIdPortIP(key0)
			epcId1, port1, ip1 := getEpcIdPortIP(key1)
			realValue, ok := value.(*GlobalEntry)
			if ok == false {
				continue
			}
			pid0, vtapId0, pid1, vtapId1 := realValue.getData()
			gpid0 := p.vtapIdAndPIDToGPID.getData(vtapId0, pid0)
			gpid1 := p.vtapIdAndPIDToGPID.getData(vtapId1, pid1)
			entry := &trident.GlobalGPIDEntry{
				Protocol: &protocol,
				VtapId_1: &vtapId1,
				EpcId_1:  &epcId1,
				Ipv4_1:   &ip1,
				Port_1:   &port1,
				Pid_1:    &pid1,
				Gpid_1:   &gpid1,
				VtapId_0: &vtapId0,
				EpcId_0:  &epcId0,
				Ipv4_0:   &ip0,
				Port_0:   &port0,
				Pid_0:    &pid0,
				Gpid_0:   &gpid0,
			}
			allData = append(allData, entry)
		}
	}

	return allData
}

type CacheReq struct {
	updateTime time.Time
	req        *trident.GPIDSyncRequest
}

func NewCacheReq(req *trident.GPIDSyncRequest) *CacheReq {
	return &CacheReq{
		updateTime: time.Now(),
		req:        req,
	}
}

func (c *CacheReq) getReq() *trident.GPIDSyncRequest {
	if c == nil {
		return nil
	}
	return c.req
}

func (c *CacheReq) getUpdateTime() int {
	if c == nil {
		return 0
	}
	return int(c.updateTime.Unix())
}

func (c *CacheReq) After(r *CacheReq) bool {
	if c == nil || r == nil {
		return false
	}
	return c.updateTime.After(r.updateTime)
}

type VTapIDToReq struct {
	sync.RWMutex
	idToReq map[uint32]*CacheReq
}

func (r *VTapIDToReq) getKeys() []uint32 {
	r.RLock()
	keys := make([]uint32, 0, len(r.idToReq))
	for key, _ := range r.idToReq {
		keys = append(keys, key)
	}
	r.RUnlock()
	return keys
}

func (r *VTapIDToReq) getSetIntKeys() mapset.Set {
	r.RLock()
	keys := mapset.NewSet()
	for key, _ := range r.idToReq {
		keys.Add(int(key))
	}
	r.RUnlock()
	return keys
}

func (r *VTapIDToReq) updateReq(req *trident.GPIDSyncRequest) {
	if req == nil {
		return
	}
	r.Lock()
	r.idToReq[req.GetVtapId()] = NewCacheReq(req)
	r.Unlock()
}

func (r *VTapIDToReq) updateCacheReq(cacheReq *CacheReq) {
	if cacheReq == nil || cacheReq.req == nil {
		return
	}

	r.Lock()
	r.idToReq[cacheReq.req.GetVtapId()] = cacheReq
	r.Unlock()
}

func (r *VTapIDToReq) getCacheReq(vtapId uint32) *CacheReq {
	r.RLock()
	cacheReq := r.idToReq[vtapId]
	r.RUnlock()
	return cacheReq
}

func (r *VTapIDToReq) getReq(vtapId uint32) *trident.GPIDSyncRequest {
	r.RLock()
	cacheReq := r.idToReq[vtapId]
	r.RUnlock()
	if cacheReq != nil {
		return cacheReq.getReq()
	}
	return nil
}

func (r *VTapIDToReq) getAllReqAndClear() map[uint32]*CacheReq {
	r.Lock()
	allData := r.idToReq
	r.idToReq = make(map[uint32]*CacheReq)
	r.Unlock()

	return allData
}

func (r *VTapIDToReq) deleteData(vtapId uint32) {
	r.Lock()
	delete(r.idToReq, vtapId)
	r.Unlock()
}

func NewVTapIDToReq() *VTapIDToReq {
	return &VTapIDToReq{
		idToReq: make(map[uint32]*CacheReq),
	}
}

// (vtap_id + pid): gpid
type IDToGPID map[uint64]uint32

func generateVPKey(vtapId uint32, pid uint32) uint64 {
	return uint64(vtapId)<<32 | uint64(pid)
}

func (p IDToGPID) getData(vtapId uint32, pid uint32) uint32 {
	return p[generateVPKey(vtapId, pid)]
}

func (p IDToGPID) addData(process *models.Process) {
	p[generateVPKey(uint32(process.VTapID), uint32(process.PID))] = uint32(process.ID)
}

type RealServerData struct {
	vtapId    uint32
	epcIdReal uint32
	portReal  uint32
	ipv4Real  uint32
	pidReal   uint32
}

func (r RealServerData) getData() (uint32, uint32, uint32, uint32, uint32) {
	return r.vtapId, r.epcIdReal, r.portReal, r.ipv4Real, r.pidReal
}

type RipToVipMap map[uint64]uint64

type RVData [MAX_SERVICE_TYPE]RipToVipMap

func NewRVData() RVData {
	var rvData RVData
	for index, _ := range rvData {
		rvData[index] = make(RipToVipMap)
	}

	return rvData
}

func (r RVData) getRVmap(protocol trident.ServiceProtocol) RipToVipMap {
	serviceIndex := MAX_SERVICE_TYPE
	switch {
	case protocol == trident.ServiceProtocol_TCP_SERVICE:
		serviceIndex = TCPService
	case protocol == trident.ServiceProtocol_UDP_SERVICE:
		serviceIndex = UDPService
	}
	if serviceIndex == MAX_SERVICE_TYPE {
		return nil
	}

	return r[serviceIndex]
}

func (r RVData) addData(epcId, rip, rport, vip, vport uint32, protocol trident.ServiceProtocol) {
	rvMap := r.getRVmap(protocol)
	if rvMap == nil {
		return
	}
	rvMap.addData(epcId, rip, rport, vip, vport)
}

func (r RVData) getVIP(repcId, rip, rport uint32, protocol trident.ServiceProtocol) (vip, vport uint32) {
	rvMap := r.getRVmap(protocol)
	if rvMap == nil {
		return
	}
	vip, vport = rvMap.getVIP(repcId, rip, rport)
	return
}

func (r RVData) getDebugData() []*trident.RipToVip {
	allData := []*trident.RipToVip{}
	for _, serviceIndex := range serviceTypes {
		var protocol trident.ServiceProtocol
		switch serviceIndex {
		case TCPService:
			protocol = trident.ServiceProtocol_TCP_SERVICE
		case UDPService:
			protocol = trident.ServiceProtocol_UDP_SERVICE
		}
		if serviceIndex >= MAX_SERVICE_TYPE {
			break
		}
		for key, value := range r[serviceIndex] {
			epcId, rport, rip := getEpcIdPortIP(key)
			_, vport, vip := getEpcIdPortIP(value)
			entry := &trident.RipToVip{
				Protocol: &protocol,
				EpcId:    &epcId,
				RIpv4:    &rip,
				RPort:    &rport,
				VIpv4:    &vip,
				VPort:    &vport,
			}
			allData = append(allData, entry)
		}
	}

	return allData

}

func (v RipToVipMap) addData(epcId, rip, rport, vip, vport uint32) {
	v[generateEPKey(epcId, rport, rip)] = generateEPKey(0, vport, vip)
}

func (v RipToVipMap) getVIP(repcId, rip, rport uint32) (vip, vport uint32) {
	key, ok := v[generateEPKey(repcId, rport, rip)]
	if ok == false {
		return
	}
	_, vport, vip = getEpcIdPortIP(key)
	return
}

type ProcessInfo struct {
	sendGPIDReq            *VTapIDToReq
	vtapIdToLocalGPIDReq   *VTapIDToReq
	vtapIdToShareGPIDReq   *VTapIDToReq
	vtapIdAndPIDToGPID     IDToGPID
	rvData                 RVData
	globalLocalEntries     EntryData
	realClientToRealServer *utils.U128IDMap
	grpcConns              map[string]*grpc.ClientConn
	db                     *gorm.DB
	config                 *config.Config
}

func NewProcessInfo(db *gorm.DB, cfg *config.Config) *ProcessInfo {
	return &ProcessInfo{
		sendGPIDReq:            NewVTapIDToReq(),
		vtapIdToLocalGPIDReq:   NewVTapIDToReq(),
		vtapIdToShareGPIDReq:   NewVTapIDToReq(),
		vtapIdAndPIDToGPID:     make(IDToGPID),
		rvData:                 NewRVData(),
		globalLocalEntries:     NewEntryData(),
		realClientToRealServer: utils.NewU128IDMapNoStats("trisolaris-real-pid", CACHE_SIZE),
		grpcConns:              make(map[string]*grpc.ClientConn),
		db:                     db,
		config:                 cfg,
	}
}

func (p *ProcessInfo) GetRealGlobalData() []*trident.RealClientToRealServer {
	data := make([]*trident.RealClientToRealServer, 0, p.realClientToRealServer.Size())

	for keyValue := range p.realClientToRealServer.Iter() {
		key0, key1, value := keyValue.GetData()
		epcId0, port0, ip0 := getEpcIdPortIP(key0)
		epcId1, port1, ip1 := getEpcIdPortIP(key1)
		realValue, ok := value.(*RealServerData)
		if ok == false {
			continue
		}
		vtapIdReal, epcIdReal, portReal, ipReal, pidReal := realValue.getData()
		etnry := &trident.RealClientToRealServer{
			EpcId_1:    &epcId1,
			Ipv4_1:     &ip1,
			Port_1:     &port1,
			EpcId_0:    &epcId0,
			Ipv4_0:     &ip0,
			Port_0:     &port0,
			EpcIdReal:  &epcIdReal,
			Ipv4Real:   &ipReal,
			PortReal:   &portReal,
			PidReal:    &pidReal,
			VtapIdReal: &vtapIdReal,
		}
		data = append(data, etnry)
	}

	return data
}

func (p *ProcessInfo) GetRVData() []*trident.RipToVip {
	return p.rvData.getDebugData()
}

func (p *ProcessInfo) getKey(entry *trident.GPIDSyncEntry) (key0, key1 uint64) {
	// server
	// If there is a real client, use the real client ip/port instead of the client ip/port
	// Use the server ip/port to query the load balancing RIP>VIP mapping table on the controller and convert it to vip/vport
	if entry.GetPid_1() > 0 && entry.GetIpv4Real() > 0 && entry.GetRoleReal() == trident.RoleType_ROLE_CLIENT {
		key0 = generateEPKey(entry.GetEpcIdReal(), entry.GetPortReal(), entry.GetIpv4Real())
		repcId, rport, ripv4 := entry.GetEpcId_1(), entry.GetPort_1(), entry.GetIpv4_1()
		vipv4, vport := p.rvData.getVIP(repcId, ripv4, rport, entry.GetProtocol())
		if vipv4 > 0 && vport > 0 {
			key1 = generateEPKey(repcId, vport, vipv4)
		} else {
			key1 = generateEPKey(repcId, rport, ripv4)
		}
	} else {
		key0 = generateEPKey(entry.GetEpcId_0(), entry.GetPort_0(), entry.GetIpv4_0())
		key1 = generateEPKey(entry.GetEpcId_1(), entry.GetPort_1(), entry.GetIpv4_1())
	}

	return
}

func (p *ProcessInfo) addRealData(vtapId uint32, entry *trident.GPIDSyncEntry, u128Map *utils.U128IDMap) {
	if entry.GetPid_1() > 0 && entry.GetIpv4Real() > 0 && entry.GetRoleReal() == trident.RoleType_ROLE_CLIENT {
		key0, key1 := p.getKey(entry)
		value := &RealServerData{
			epcIdReal: entry.GetEpcId_1(),
			portReal:  entry.GetPort_1(),
			ipv4Real:  entry.GetIpv4_1(),
			pidReal:   entry.GetPid_1(),
			vtapId:    vtapId,
		}
		u128Map.AddOrGet(key0, key1, value, true)
	}
}

func (p *ProcessInfo) getRealData(entry *trident.GPIDSyncEntry) *RealServerData {
	key0, key1 := p.getKey(entry)
	realData, ok := p.realClientToRealServer.Get(key0, key1)
	if ok {
		return realData.(*RealServerData)
	}

	return nil
}

func (p *ProcessInfo) UpdateVTapGPIDReq(req *trident.GPIDSyncRequest) {
	p.sendGPIDReq.updateReq(req)
}

func (p *ProcessInfo) GetVTapGPIDReq(vtapId uint32) (*trident.GPIDSyncRequest, uint32) {
	cacheReq := p.sendGPIDReq.getCacheReq(vtapId)
	if cacheReq == nil {
		localReq := p.vtapIdToLocalGPIDReq.getCacheReq(vtapId)
		shareReq := p.vtapIdToShareGPIDReq.getCacheReq(vtapId)
		if localReq != nil && shareReq != nil {
			if localReq.After(shareReq) {
				cacheReq = localReq
			} else {
				cacheReq = shareReq
			}
		} else {
			if localReq == nil {
				cacheReq = shareReq
			} else {
				cacheReq = localReq
			}
		}
	}

	return cacheReq.getReq(), uint32(cacheReq.getUpdateTime())
}

func (p *ProcessInfo) UpdateGPIDReqFromShare(shareReq *trident.ShareGPIDSyncRequests) {
	for _, req := range shareReq.GetSyncRequests() {
		p.vtapIdToShareGPIDReq.updateReq(req)
	}
}

func (p *ProcessInfo) GetGPIDShareReqs() *trident.ShareGPIDSyncRequests {
	reqs := p.sendGPIDReq.getAllReqAndClear()
	shareSyncReqs := make([]*trident.GPIDSyncRequest, 0, len(reqs))
	for _, req := range reqs {
		p.vtapIdToLocalGPIDReq.updateCacheReq(req)
		shareSyncReqs = append(shareSyncReqs, req.getReq())
	}
	if len(shareSyncReqs) > 0 {
		return &trident.ShareGPIDSyncRequests{
			ServerIp:     proto.String(p.config.NodeIP),
			SyncRequests: shareSyncReqs,
		}
	}
	return nil
}

func (p *ProcessInfo) updateGlobalLocalEntries(data EntryData) {
	p.globalLocalEntries = data
}

func (p *ProcessInfo) updateRealClientToRealServer(data *utils.U128IDMap) {
	p.realClientToRealServer = data
}

func (p *ProcessInfo) GetGlobalEntries() []*trident.GlobalGPIDEntry {
	return p.globalLocalEntries.getGPIDGlobalData(p)
}

func (p *ProcessInfo) generateGlobalLocalEntries() {
	globalLocalEntries := NewEntryData()
	realClientToRealServer := utils.NewU128IDMapNoStats("trisolaris-real-pid", CACHE_SIZE)
	vtapIds := p.vtapIdToLocalGPIDReq.getKeys()
	shareFilter := mapset.NewSet()
	for _, vtapId := range vtapIds {
		localCacheReq := p.vtapIdToLocalGPIDReq.getCacheReq(vtapId)
		if localCacheReq == nil {
			continue
		}
		shareCacheReq := p.vtapIdToShareGPIDReq.getCacheReq(vtapId)
		if shareCacheReq != nil {
			if shareCacheReq.After(localCacheReq) {
				continue
			} else {
				shareFilter.Add(vtapId)
			}
		}

		req := localCacheReq.getReq()
		if req == nil || len(req.GetEntries()) == 0 {
			continue
		}
		for _, entry := range req.GetEntries() {
			globalLocalEntries.addData(vtapId, entry, p)
			p.addRealData(vtapId, entry, realClientToRealServer)
		}
	}

	vtapIds = p.vtapIdToShareGPIDReq.getKeys()
	for _, vtapId := range vtapIds {
		if shareFilter.Contains(vtapId) {
			continue
		}
		req := p.vtapIdToShareGPIDReq.getReq(vtapId)
		if req == nil {
			continue
		}
		if len(req.GetEntries()) == 0 {
			continue
		}
		for _, entry := range req.GetEntries() {
			globalLocalEntries.addData(vtapId, entry, p)
			p.addRealData(vtapId, entry, realClientToRealServer)
		}
	}

	p.updateGlobalLocalEntries(globalLocalEntries)
	p.updateRealClientToRealServer(realClientToRealServer)
}

func (p *ProcessInfo) getGPIDInfoFromDB() {
	processes, err := dbmgr.DBMgr[models.Process](p.db).GetFields([]string{"id", "vtap_id", "pid"})
	if err != nil {
		log.Error(err)
		return
	}
	newVtapIDAndPIDToGPID := make(IDToGPID)
	for _, process := range processes {
		newVtapIDAndPIDToGPID.addData(process)
	}
	p.vtapIdAndPIDToGPID = newVtapIDAndPIDToGPID
}

func (p *ProcessInfo) getRipToVipFromDB() {
	rvData := NewRVData()
	idTolbListener := make(map[int]*models.LBListener)
	lbListeners, err := dbmgr.DBMgr[models.LBListener](p.db).Gets()
	if err != nil {
		log.Error(err)
		return
	}
	for _, lbListener := range lbListeners {
		idTolbListener[lbListener.ID] = lbListener
	}

	lbTargetServers, err := dbmgr.DBMgr[models.LBTargetServer](p.db).Gets()
	if err != nil {
		log.Error(err)
		return
	}
	for _, lbTargetServer := range lbTargetServers {
		if lbListener, ok := idTolbListener[lbTargetServer.LBListenerID]; ok {
			ripNet := libu.ParserStringIp(lbTargetServer.IP)
			if ripNet == nil {
				continue
			}
			vips := strings.Split(lbListener.IPs, ",")
			for _, vip := range vips {
				vipNet := libu.ParserStringIp(vip)
				if vipNet == nil {
					continue
				}
				epcId := uint32(lbTargetServer.VPCID)
				rip := libu.IpToUint32(ripNet)
				rport := uint32(lbTargetServer.Port)
				vip := libu.IpToUint32(vipNet)
				vport := uint32(lbListener.Port)
				rvData.addData(epcId, rip, rport, vip, vport, convertProto(lbListener.Protocol))
			}
		}
	}
	p.rvData = rvData
	log.Info(p.rvData)
}

func (p *ProcessInfo) GetGPIDResponseByVtapID(vtapId uint32) *trident.GPIDSyncResponse {
	req, _ := p.GetVTapGPIDReq(vtapId)
	return p.GetGPIDResponseByReq(req)
}

func (p *ProcessInfo) GetGPIDResponseByReq(req *trident.GPIDSyncRequest) *trident.GPIDSyncResponse {
	if req == nil {
		return &trident.GPIDSyncResponse{}
	}
	entries := req.GetEntries()
	if len(entries) == 0 {
		return &trident.GPIDSyncResponse{}
	}
	vtapId := req.GetVtapId()
	responseEntries := make([]*trident.GPIDSyncEntry, 0, len(entries))
	for _, entry := range entries {
		netnsIndex := entry.GetNetnsIdx()
		roleReal := entry.GetRoleReal()
		protocol := entry.GetProtocol()
		responseEntry := &trident.GPIDSyncEntry{
			Protocol:  &protocol,
			RoleReal:  &roleReal,
			EpcId_1:   proto.Uint32(entry.GetEpcId_1()),
			Ipv4_1:    proto.Uint32(entry.GetIpv4_1()),
			Port_1:    proto.Uint32(entry.GetPort_1()),
			EpcId_0:   proto.Uint32(entry.GetEpcId_0()),
			Ipv4_0:    proto.Uint32(entry.GetIpv4_0()),
			Port_0:    proto.Uint32(entry.GetPort_0()),
			EpcIdReal: proto.Uint32(entry.GetEpcIdReal()),
			Ipv4Real:  proto.Uint32(entry.GetIpv4Real()),
			PortReal:  proto.Uint32(entry.GetPortReal()),
			NetnsIdx:  &netnsIndex,
		}
		var gpid0, gpid1, gpidReal uint32
		globalEntry := p.globalLocalEntries.getData(entry, p)
		if entry.GetPid_0() > 0 {
			gpid0 = p.vtapIdAndPIDToGPID.getData(vtapId, entry.GetPid_0())
		} else if globalEntry != nil {
			pid0, vtapId0 := globalEntry.getPid0Data()
			if pid0 > 0 && vtapId0 > 0 {
				gpid0 = p.vtapIdAndPIDToGPID.getData(vtapId0, pid0)
			}
		}
		if entry.GetPid_1() > 0 {
			gpid1 = p.vtapIdAndPIDToGPID.getData(vtapId, entry.GetPid_1())
		} else if globalEntry != nil {
			pid1, vtapId1 := globalEntry.getPid1Data()
			if pid1 > 0 && vtapId1 > 0 {
				gpid1 = p.vtapIdAndPIDToGPID.getData(vtapId1, pid1)
			}
		}

		if entry.GetPidReal() > 0 {
			gpid1 = p.vtapIdAndPIDToGPID.getData(vtapId, entry.GetPidReal())
		} else if entry.GetPidReal() == 0 && entry.GetIpv4Real() > 0 {
			if globalEntry != nil {
				pid, vtapId := globalEntry.getPid0Data()
				if pid > 0 && vtapId > 0 {
					gpidReal = p.vtapIdAndPIDToGPID.getData(vtapId, pid)
				}
			}
		}

		if entry.GetIpv4Real() == 0 {
			realServerData := p.getRealData(entry)
			if realServerData != nil {
				vtapIdReal, epcIdReal, portReal, ipv4Real, pidReal := realServerData.getData()
				role := trident.RoleType_ROLE_SERVER
				responseEntry.EpcIdReal = &epcIdReal
				responseEntry.Ipv4Real = &ipv4Real
				responseEntry.PortReal = &portReal
				responseEntry.RoleReal = &role
				gpidReal = p.vtapIdAndPIDToGPID.getData(vtapIdReal, pidReal)
			}
		}

		responseEntry.Pid_0 = &gpid0
		responseEntry.Pid_1 = &gpid1
		responseEntry.PidReal = &gpidReal
		responseEntries = append(responseEntries, responseEntry)
	}
	return &trident.GPIDSyncResponse{Entries: responseEntries}
}

func (p *ProcessInfo) DeleteVTapExpiredData(dbVTapIDs mapset.Set) {
	cacheVTapIDs := p.vtapIdToLocalGPIDReq.getSetIntKeys()
	delVTapIDs := cacheVTapIDs.Difference(dbVTapIDs)
	for val := range delVTapIDs.Iter() {
		vtapId := val.(int)
		p.vtapIdToLocalGPIDReq.deleteData(uint32(vtapId))
	}

	cacheVTapIDs = p.vtapIdToShareGPIDReq.getSetIntKeys()
	delVTapIDs = cacheVTapIDs.Difference(dbVTapIDs)
	for val := range delVTapIDs.Iter() {
		vtapId := val.(int)
		p.vtapIdToShareGPIDReq.deleteData(uint32(vtapId))
	}
}

func (p *ProcessInfo) getLocalControllersConns() map[string]*grpc.ClientConn {
	controllerIPToRegion := make(map[string]string)
	localRegion := ""
	conns, err := dbmgr.DBMgr[models.AZControllerConnection](p.db).Gets()
	if err != nil {
		log.Errorf("get az_controller_conn failed, err:%s", err)
		return nil
	}
	for _, conn := range conns {
		controllerIPToRegion[conn.ControllerIP] = conn.Region
		if p.config.NodeIP == conn.ControllerIP {
			localRegion = conn.Region
		}
	}
	dbControllers, err := dbmgr.DBMgr[models.Controller](p.db).Gets()
	if err != nil {
		log.Errorf("get controller failed, err:%s", err)
		return nil
	}
	localControllers := map[string]struct{}{}
	for _, controller := range dbControllers {
		if controller.IP == p.config.NodeIP {
			continue
		}
		if controller.State != HOST_STATE_EXCEPTION {
			if controllerIPToRegion[controller.IP] == localRegion {
				serverIP := controller.PodIP
				if serverIP == "" {
					serverIP = controller.IP
				}
				localControllers[serverIP] = struct{}{}
				if _, ok := p.grpcConns[serverIP]; ok {
					continue
				}
				serverAddr := net.JoinHostPort(serverIP, strconv.Itoa(p.config.GetGrpcPort()))
				conn, err := grpc.Dial(serverAddr, grpc.WithInsecure(),
					grpc.WithMaxMsgSize(p.config.GetGrpcMaxMessageLength()))
				if err != nil {
					log.Error("failed to start gRPC connection(%s): %v", err)
					continue
				}
				p.grpcConns[serverIP] = conn
			}
		}
	}

	for serverIP, grpcConn := range p.grpcConns {
		if _, ok := localControllers[serverIP]; !ok {
			grpcConn.Close()
			delete(p.grpcConns, serverIP)
		}
	}

	return p.grpcConns
}

func (p *ProcessInfo) sendLocalShareEntryData() {
	grpcConns := p.getLocalControllersConns()
	if len(grpcConns) == 0 {
		for _, cacheReq := range p.sendGPIDReq.getAllReqAndClear() {
			p.vtapIdToLocalGPIDReq.updateCacheReq(cacheReq)
		}

		return
	}
	shareReqs := p.GetGPIDShareReqs()
	if shareReqs == nil {
		return
	}
	for _, conn := range grpcConns {
		go func(conn *grpc.ClientConn) {
			log.Infof("server(%s) send local share req data to server(%s)", p.config.NodeIP, conn.Target())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := trident.NewSynchronizerClient(conn)
			response, err := client.ShareGPIDLocalData(ctx, shareReqs)
			if err != nil {
				log.Error(err)
				return
			}
			if len(response.GetSyncRequests()) == 0 {
				return
			}
			log.Infof("receive gpid sync data from server(%s)", response.GetServerIp())
			for _, req := range response.GetSyncRequests() {
				p.vtapIdToShareGPIDReq.updateReq(req)
			}
		}(conn)
	}
}

func (p *ProcessInfo) getDBData() {
	p.getGPIDInfoFromDB()
	p.getRipToVipFromDB()
}

func (p *ProcessInfo) generateData() {
	p.sendLocalShareEntryData()
	p.getDBData()
	p.generateGlobalLocalEntries()
}

func (p *ProcessInfo) TimedGenerateGPIDInfo() {
	p.getDBData()
	interval := time.Duration(p.config.GPIDRefreshInterval)
	ticker := time.NewTicker(interval * time.Second).C
	for {
		select {
		case <-ticker:
			log.Info("start generate gpid data from timed")
			p.generateData()
			log.Info("end generate gpid data from timed")
		}
	}
}
