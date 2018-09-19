package labeler

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/google/gopacket/layers"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
	"gitlab.x.lan/yunshan/droplet-libs/dropletpb"
	"gitlab.x.lan/yunshan/droplet-libs/policy"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
	. "gitlab.x.lan/yunshan/droplet-libs/utils"

	"gitlab.x.lan/yunshan/droplet/dropletctl"
	"gitlab.x.lan/yunshan/message/trident"
)

var log = logging.MustGetLogger("labeler")

type QueueType uint8

const (
	QUEUE_TYPE_FLOW QueueType = iota
	QUEUE_TYPE_METERING
	QUEUE_TYPE_MAX
)

type LabelerManager struct {
	policyTable     *policy.PolicyTable
	readQueues      queue.MultiQueueReader
	readQueuesCount int
	appQueues       [QUEUE_TYPE_MAX]queue.MultiQueueWriter
	running         bool
}

const (
	LABELER_CMD_DUMP_PLATFORM = iota
	LABELER_CMD_DUMP_ACL
)

type DumpKey struct {
	Mac    uint64
	Ip     uint32
	InPort uint32
}

func NewLabelerManager(readQueues queue.MultiQueueReader, count int, size uint32) *LabelerManager {
	labeler := &LabelerManager{
		policyTable:     policy.NewPolicyTable(datatype.ACTION_FLOW_STAT, count, size),
		readQueues:      readQueues,
		readQueuesCount: count,
	}
	dropletctl.Register(dropletctl.DROPLETCTL_LABELER, labeler)
	stats.RegisterCountable("labeler", labeler)
	return labeler
}

func (l *LabelerManager) GetCounter() interface{} {
	return l.policyTable.GetCounter()
}

func (l *LabelerManager) RegisterAppQueue(queueType QueueType, appQueues queue.MultiQueueWriter) {
	l.appQueues[queueType] = appQueues
}

func (l *LabelerManager) OnAclDataChange(response *trident.SyncResponse) {
	if plarformData := response.GetPlatformData(); plarformData != nil {
		if interfaces := plarformData.GetInterfaces(); interfaces != nil {
			l.OnPlatformDataChange(dropletpb.Convert2PlatformData(response))
		} else {
			l.OnPlatformDataChange(nil)
		}
		if ipGroups := plarformData.GetIpGroups(); ipGroups != nil {
			l.OnIpGroupDataChange(dropletpb.Convert2IpGroupdata(response))
		} else {
			l.OnIpGroupDataChange(nil)
		}
	} else {
		l.OnPlatformDataChange(nil)
		l.OnIpGroupDataChange(nil)
	}

	if flowAcls := response.GetFlowAcls(); flowAcls != nil {
		l.OnPolicyDataChange(dropletpb.Convert2AclData(response))
	} else {
		l.OnPolicyDataChange(nil)
	}
}

func (l *LabelerManager) OnPlatformDataChange(data []*datatype.PlatformData) {
	l.policyTable.UpdateInterfaceData(data)
}

func (l *LabelerManager) OnIpGroupDataChange(data []*policy.IpGroupData) {
	l.policyTable.UpdateIpGroupData(data)
}

func (l *LabelerManager) OnPolicyDataChange(data []*policy.Acl) {
	l.policyTable.UpdateAclData(data)
}

func GetTapType(inPort uint32) datatype.TapType {
	if policy.PortInDeepflowExporter(inPort) {
		return datatype.TAP_TOR
	}
	return datatype.TAP_ISP
}

func (l *LabelerManager) GetPolicy(packet *datatype.MetaPacket, index int) *datatype.PolicyData {
	key := &datatype.LookupKey{
		SrcMac:    uint64(packet.MacSrc),
		DstMac:    uint64(packet.MacDst),
		SrcIp:     uint32(packet.IpSrc),
		DstIp:     uint32(packet.IpDst),
		SrcPort:   packet.PortSrc,
		DstPort:   packet.PortDst,
		EthType:   packet.EthType,
		Vlan:      packet.Vlan,
		Proto:     uint8(packet.Protocol),
		Ttl:       packet.TTL,
		L2End0:    packet.L2End0,
		L2End1:    packet.L2End1,
		Tap:       GetTapType(packet.InPort),
		Invalid:   packet.Invalid,
		FastIndex: index,
	}

	packet.EndpointData, packet.PolicyData = l.policyTable.LookupAllByKey(key, index)
	log.Debug("QUERY PACKET:", packet, "ENDPOINTDATA:", packet.EndpointData, "POLICYDATA:", packet.PolicyData)
	return packet.PolicyData
}

func (l *LabelerManager) run(index int) {
	meteringQueues := l.appQueues[QUEUE_TYPE_METERING]
	flowQueues := l.appQueues[QUEUE_TYPE_FLOW]
	size := 4096
	meteringKeys := make([]queue.HashKey, 0, size)
	flowKeys := make([]queue.HashKey, 0, size)
	meteringItemBatch := make([]interface{}, 0, size)
	flowItemBatch := make([]interface{}, 0, size)
	itemBatch := make([]interface{}, size)

	for l.running {
		itemCount := l.readQueues.Gets(queue.HashKey(index), itemBatch)
		for _, item := range itemBatch[:itemCount] {
			metaPacket := item.(*datatype.MetaPacket)
			action := l.GetPolicy(metaPacket, index)
			if (action.ActionList & datatype.ACTION_PACKET_STAT) != 0 {
				meteringItemBatch = append(meteringItemBatch, metaPacket)
				meteringKeys = append(meteringKeys, queue.HashKey(metaPacket.Hash))
			}
			if (action.ActionList & datatype.ACTION_FLOW_STAT) != 0 {
				flowItemBatch = append(flowItemBatch, metaPacket)
				flowKeys = append(flowKeys, queue.HashKey(metaPacket.Hash))
			}
		}
		if len(meteringItemBatch) > 0 {
			meteringQueues.Puts(meteringKeys, meteringItemBatch)
			meteringKeys = meteringKeys[:0]
			meteringItemBatch = meteringItemBatch[:0]
		}
		if len(flowItemBatch) > 0 {
			flowQueues.Puts(flowKeys, flowItemBatch)
			flowKeys = flowKeys[:0]
			flowItemBatch = flowItemBatch[:0]
		}
	}

	log.Info("Labeler manager exit")
}

func (l *LabelerManager) Start() {
	if !l.running {
		l.running = true
		log.Info("Start labeler manager")
		for i := 0; i < l.readQueuesCount; i++ {
			go l.run(i)
		}
	}
}

func (l *LabelerManager) Stop(wait bool) {
	if l.running {
		log.Info("Stop labeler manager")
		l.running = false
	}
}

func (l *LabelerManager) recvDumpPlatform(conn *net.UDPConn, port int, arg *bytes.Buffer) {
	key := DumpKey{}
	buffer := bytes.Buffer{}

	decoder := gob.NewDecoder(arg)
	if err := decoder.Decode(&key); err != nil {
		log.Error(err)
		dropletctl.SendToDropletCtl(conn, port, 1, nil)
		return
	}

	info := l.policyTable.GetEndpointInfo(key.Mac, key.Ip, key.InPort)
	if info == nil {
		log.Warningf("GetEndpointInfo(%+v) return nil", key)
		dropletctl.SendToDropletCtl(conn, port, 1, nil)
		return
	}
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(info); err != nil {
		log.Errorf("encoder.Encode: %s", err)
		dropletctl.SendToDropletCtl(conn, port, 1, nil)
		return
	}
	dropletctl.SendToDropletCtl(conn, port, 0, &buffer)
}

func (l *LabelerManager) recvDumpAcl(conn *net.UDPConn, port int, arg *bytes.Buffer) {
	key := datatype.LookupKey{}
	buffer := bytes.Buffer{}

	decoder := gob.NewDecoder(arg)
	if err := decoder.Decode(&key); err != nil {
		log.Error(err)
		dropletctl.SendToDropletCtl(conn, port, 1, nil)
		return
	}

	endpoint, policy := l.policyTable.LookupAllByKey(&key, 0)
	info := fmt.Sprintf("EndPoint: {Src: %+v Dst: %+v} Policy: %+v", endpoint.SrcInfo, endpoint.DstInfo, policy)
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(info); err != nil {
		log.Errorf("encoder.Encode: %s", err)
		dropletctl.SendToDropletCtl(conn, port, 1, nil)
		return
	}
	dropletctl.SendToDropletCtl(conn, port, 0, &buffer)
}

func (l *LabelerManager) RecvCommand(conn *net.UDPConn, port int, operate uint16, arg *bytes.Buffer) {
	switch operate {
	case LABELER_CMD_DUMP_PLATFORM:
		l.recvDumpPlatform(conn, port, arg)
	case LABELER_CMD_DUMP_ACL:
		l.recvDumpAcl(conn, port, arg)
	}
}

func parseUint(s string) (uint32, error) {
	if s[0:2] == "0x" {
		x, err := strconv.ParseUint(s[2:], 16, 64)
		return uint32(x), err
	} else {
		x, err := strconv.ParseUint(s, 10, 64)
		return uint32(x), err
	}
}

func parseTapType(s string) datatype.TapType {
	switch s {
	case "isp":
		return datatype.TAP_ISP
	case "tor":
		return datatype.TAP_TOR
	default:
		return 0
	}
}

func newLookupKey(cmdLine string) *datatype.LookupKey {
	key := &datatype.LookupKey{}
	keyValues := strings.Split(cmdLine, ",")
	for _, keyValue := range keyValues {
		parts := strings.Split(keyValue, "=")
		switch parts[0] {
		case "tap":
			key.Tap = parseTapType(parts[1])
			if key.Tap != datatype.TAP_TOR && key.Tap != datatype.TAP_ISP {
				fmt.Printf("unknown tap type from: %s\n", cmdLine)
				return nil
			}
		case "inport":
			inport, err := parseUint(parts[1])
			if err != nil {
				fmt.Printf("%s: %v\n", cmdLine, err)
				return nil
			}
			key.RxInterface = inport
		case "smac":
			mac, err := net.ParseMAC(parts[1])
			if err != nil {
				fmt.Printf("unknown mac address from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.SrcMac = Mac2Uint64(mac)
		case "dmac":
			mac, err := net.ParseMAC(parts[1])
			if err != nil {
				fmt.Printf("unknown mac address from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.DstMac = Mac2Uint64(mac)
		case "vlan":
			vlan, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("unknown vlan from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.Vlan = uint16(vlan)
		case "eth_type":
			ethType, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("unknown eth_type from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.EthType = layers.EthernetType(ethType)
		case "sip":
			key.SrcIp = IpToUint32(net.ParseIP(parts[1]))
		case "dip":
			key.DstIp = IpToUint32(net.ParseIP(parts[1]))
		case "proto":
			proto, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("unknown proto from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.Proto = uint8(proto)
		case "sport":
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("unknown port from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.SrcPort = uint16(port)
		case "dport":
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("unknown port from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.DstPort = uint16(port)
		default:
			fmt.Printf("unknown key %s from %s\n", parts[0], cmdLine)
			return nil
		}
	}
	return key
}

func newDumpKey(cmdLine string) *DumpKey {
	key := &DumpKey{}
	keyValues := strings.Split(cmdLine, ",")
	for _, keyValue := range keyValues {
		parts := strings.Split(keyValue, "=")
		switch parts[0] {
		case "mac":
			mac, err := net.ParseMAC(parts[1])
			if err != nil {
				fmt.Printf("unknown mac address from: %s[%v]\n", cmdLine, err)
				return nil
			}
			key.Mac = Mac2Uint64(mac)
		case "ip":
			key.Ip = IpToUint32(net.ParseIP(parts[1]))
		case "inport":
			inport, err := parseUint(parts[1])
			if err != nil {
				fmt.Printf("%s: %v\n", cmdLine, err)
				return nil
			}
			key.InPort = inport
		default:
			fmt.Printf("unknown key %s from %s\n", parts[0], cmdLine)
			return nil
		}
	}
	return key
}

func sendLookupKey(cmdLine string) (*bytes.Buffer, error) {
	key := newLookupKey(cmdLine)
	if key == nil {
		return nil, errors.New("input error!")
	}
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(key); err != nil {
		return nil, err
	}
	_, result, err := dropletctl.SendToDroplet(dropletctl.DROPLETCTL_LABELER, LABELER_CMD_DUMP_ACL, &buffer)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func sendDumpKey(cmdLine string) (*bytes.Buffer, error) {
	key := newDumpKey(cmdLine)
	if key == nil {
		return nil, errors.New("input error!")
	}
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(key); err != nil {
		return nil, err
	}
	_, result, err := dropletctl.SendToDroplet(dropletctl.DROPLETCTL_LABELER, LABELER_CMD_DUMP_PLATFORM, &buffer)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func dumpPlatform(cmdLine string) {
	result, err := sendDumpKey(cmdLine)
	if err != nil {
		fmt.Println(err)
		return
	}
	info := datatype.EndpointInfo{}
	decoder := gob.NewDecoder(result)
	if err := decoder.Decode(&info); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("%s:\n\t%+v\n", cmdLine, info)
}

func dumpAcl(cmdLine string) {
	result, err := sendLookupKey(cmdLine)
	if err != nil {
		fmt.Println(err)
		return
	}
	var info string
	decoder := gob.NewDecoder(result)
	if err := decoder.Decode(&info); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("%s:\n\t%+v\n", cmdLine, info)
}

func RegisterCommand() *cobra.Command {
	labeler := &cobra.Command{
		Use:   "labeler",
		Short: "config droplet labeler module",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("please run with arguments 'dump-platform'.\n")
		},
	}
	dump := &cobra.Command{
		Use:     "dump-platform [filter]",
		Short:   "dump platform data infomation",
		Example: "droplet-ctl labeler dump-platform inport=1000,mac=12:34:56:78:9a:bc,ip=127.0.0.1",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Printf("filter is nil, Example: %s\n", cmd.Example)
				return
			}
			dumpPlatform(args[0])
		},
	}
	dumpAcl := &cobra.Command{
		Use:   "dump-acl {filter}",
		Short: "show policy list",
		Long: "droplet-ctl labeler dump-acl {[key=value]+}\n" +
			"key list:\n" +
			"\ttap         use 'isp|tor'\n" +
			"\tinport      capture interface mac suffix\n" +
			"\tsmac/dmac   packet mac address\n" +
			"\teth_type    packet eth type\n" +
			"\tvlan        packet vlan\n" +
			"\tsip/dip     packet ip address\n" +
			"\tproto       packet ip proto\n" +
			"\tsport/dport packet port",
		Example: "droplet-ctl labeler dump-acl inport=0x10000,smac=12:34:56:78:9a:bc,sip=127.0.0.1",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Printf("filter is nil, Example: %s\n", cmd.Example)
				return
			}
			dumpAcl(args[0])
		},
	}
	labeler.AddCommand(dump)
	labeler.AddCommand(dumpAcl)
	return labeler
}
