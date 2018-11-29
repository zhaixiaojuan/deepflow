package mapreduce

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"time"

	"gitlab.x.lan/application/droplet-app/pkg/mapper/consolelog"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flow"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/flowtype"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/fps"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/geo"
	"gitlab.x.lan/application/droplet-app/pkg/mapper/perf"
	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
)

const (
	perfSlots = 2
)

const GEO_FILE_LOCATION = "/usr/share/droplet/ip_info_mini.json"

func NewFlowMapProcess(output queue.MultiQueueWriter, input queue.MultiQueueReader, inputCount int, docsInBuffer int, windowSize int) *FlowHandler {
	return NewFlowHandler([]app.FlowProcessor{
		fps.NewProcessor(),
		flow.NewProcessor(),
		perf.NewProcessor(),
		geo.NewProcessor(GEO_FILE_LOCATION),
		flowtype.NewProcessor(),
		consolelog.NewProcessor(),
	}, output, input, inputCount, docsInBuffer, windowSize)
}

type FlowHandler struct {
	numberOfApps int
	processors   []app.FlowProcessor

	flowQueue      queue.MultiQueueReader
	flowQueueCount int
	zmqAppQueue    queue.MultiQueueWriter
	docsInBuffer   int
	windowSize     int
}

func NewFlowHandler(processors []app.FlowProcessor, output queue.MultiQueueWriter, inputs queue.MultiQueueReader, inputCount int, docsInBuffer int, windowSize int) *FlowHandler {
	return &FlowHandler{
		numberOfApps:   len(processors),
		processors:     processors,
		zmqAppQueue:    output,
		flowQueue:      inputs,
		flowQueueCount: inputCount,
		docsInBuffer:   docsInBuffer,
		windowSize:     windowSize,
	}
}

type subFlowHandler struct {
	numberOfApps int
	processors   []app.FlowProcessor
	stashes      []*Stash

	flowQueue   queue.MultiQueueReader
	zmqAppQueue queue.MultiQueueWriter

	queueIndex int
	hashKey    queue.HashKey

	counterLatch int
	statItems    []stats.StatItem

	lastFlush     time.Duration
	statsdCounter []StatsdCounter
}

type StatsdCounter struct {
	docCounter  uint64
	flowCounter uint64
	emitCounter uint64
	maxCounter  uint64
}

func (h *FlowHandler) newSubFlowHandler(index int) *subFlowHandler {
	dupProcessors := make([]app.FlowProcessor, h.numberOfApps)
	for i, proc := range h.processors {
		elem := reflect.ValueOf(proc).Elem()
		ref := reflect.New(elem.Type())
		ref.Elem().Set(elem)
		dupProcessors[i] = ref.Interface().(app.FlowProcessor)
		dupProcessors[i].Prepare()
	}
	handler := subFlowHandler{
		numberOfApps: h.numberOfApps,
		processors:   dupProcessors,
		stashes:      make([]*Stash, h.numberOfApps),

		flowQueue:   h.flowQueue,
		zmqAppQueue: h.zmqAppQueue,

		queueIndex: index,
		hashKey:    queue.HashKey(rand.Int()),

		counterLatch: 0,
		statItems:    make([]stats.StatItem, h.numberOfApps*3),

		lastFlush: time.Duration(time.Now().UnixNano()),

		statsdCounter: make([]StatsdCounter, h.numberOfApps*2),
	}

	for i := 0; i < handler.numberOfApps; i++ {
		handler.stashes[i] = NewStash(h.docsInBuffer, h.windowSize)
		handler.statItems[i].Name = h.processors[i].GetName()
		handler.statItems[i].StatType = stats.COUNT_TYPE
		handler.statItems[i+handler.numberOfApps].Name = fmt.Sprintf("%s_avg_doc_counter", h.processors[i].GetName())
		handler.statItems[i+handler.numberOfApps].StatType = stats.COUNT_TYPE
		handler.statItems[i+handler.numberOfApps*2].Name = fmt.Sprintf("%s_max_doc_counter", h.processors[i].GetName())
		handler.statItems[i+handler.numberOfApps*2].StatType = stats.COUNT_TYPE
	}
	stats.RegisterCountable("flow_mapper", &handler, stats.OptionStatTags{"index": strconv.Itoa(index)})
	return &handler
}

func (f *subFlowHandler) GetCounter() interface{} {
	oldLatch := f.counterLatch
	if f.counterLatch == 0 {
		f.counterLatch = f.numberOfApps
	} else {
		f.counterLatch = 0
	}
	for i := 0; i < f.numberOfApps; i++ {
		f.statItems[i].Value = f.statsdCounter[i+oldLatch].emitCounter
		if f.statsdCounter[i+oldLatch].flowCounter != 0 {
			f.statItems[i+f.numberOfApps].Value = f.statsdCounter[i+oldLatch].docCounter / f.statsdCounter[i+oldLatch].flowCounter
		} else {
			f.statItems[i+f.numberOfApps].Value = 0
		}
		f.statItems[i+f.numberOfApps*2].Value = f.statsdCounter[i+oldLatch].maxCounter
		f.statsdCounter[i+oldLatch].emitCounter = 0
		f.statsdCounter[i+oldLatch].docCounter = 0
		f.statsdCounter[i+oldLatch].flowCounter = 0
		f.statsdCounter[i+oldLatch].maxCounter = 0
	}

	return f.statItems
}

func (f *subFlowHandler) putToQueue() {
	for i, stash := range f.stashes {
		docs := stash.Dump()
		for j := 0; j < len(docs); j += QUEUE_BATCH_SIZE {
			if j+QUEUE_BATCH_SIZE <= len(docs) {
				f.zmqAppQueue.Put(f.hashKey, docs[j:j+QUEUE_BATCH_SIZE]...)
			} else {
				f.zmqAppQueue.Put(f.hashKey, docs[j:]...)
			}
			f.hashKey++
		}
		f.statsdCounter[i+f.counterLatch].emitCounter += uint64(len(docs))
		stash.Clear()
	}
}

func (f *FlowHandler) Start() {
	for i := 0; i < f.flowQueueCount; i++ {
		go f.newSubFlowHandler(i).Process()
	}
}

func isValidFlow(flow *datatype.TaggedFlow) bool {
	startTime := flow.StartTime
	endTime := flow.EndTime
	// we give flow timestamp a tolerance with one minute
	toleranceCurTime := time.Duration(time.Now().UnixNano()) + time.Minute

	if startTime > toleranceCurTime || endTime > toleranceCurTime {
		return false
	}
	if endTime != 0 && endTime < startTime {
		return false
	}

	rightMargin := endTime + 2*time.Minute
	times := [3]time.Duration{
		flow.CurStartTime,
		flow.FlowMetricsPeerSrc.ArrTimeLast,
		flow.FlowMetricsPeerDst.ArrTimeLast,
	}
	for i := 0; i < 3; i++ {
		if times[i] > rightMargin || times[i] > toleranceCurTime {
			return false
		}
	}
	return true
}

func (f *subFlowHandler) Process() error {
	elements := make([]interface{}, QUEUE_BATCH_SIZE)

	for {
		n := f.flowQueue.Gets(queue.HashKey(f.queueIndex), elements)
		for _, e := range elements[:n] {
			if e == nil {
				f.Flush()
				continue
			}

			flow := e.(*datatype.TaggedFlow)
			if !isValidFlow(flow) {
				datatype.ReleaseTaggedFlow(flow)
				log.Warning("flow timestamp incorrect and dropped")
				continue
			}

			for i, processor := range f.processors {
				docs := processor.Process(flow, false)
				f.statsdCounter[i+f.counterLatch].docCounter += uint64(len(docs))
				f.statsdCounter[i+f.counterLatch].flowCounter++
				if uint64(len(docs)) > f.statsdCounter[i+f.counterLatch].maxCounter {
					f.statsdCounter[i+f.counterLatch].maxCounter = uint64(len(docs))
				}
				for {
					docs = f.stashes[i].Add(docs)
					if docs == nil {
						break
					}
					f.Flush()
				}
			}
			datatype.ReleaseTaggedFlow(flow)
		}
		if time.Duration(time.Now().UnixNano())-f.lastFlush >= FLUSH_INTERVAL {
			f.Flush()
		}
	}
}

func (f *subFlowHandler) Flush() {
	f.lastFlush = time.Duration(time.Now().UnixNano())
	f.putToQueue()
}
