package sender

import (
	"strconv"

	"gitlab.x.lan/yunshan/droplet-libs/queue"
	"gitlab.x.lan/yunshan/droplet-libs/stats"
	"gitlab.x.lan/yunshan/droplet-libs/utils"
)

type ZeroDocumentSender struct {
	inputQueues []queue.MultiQueueReader
	queueCounts []int
	ips         []string
	ports       []uint16
}

type zeroDocumentSenderBuilder struct {
	inputQueues []queue.MultiQueueReader
	queueCounts []int
	ips         []string
	ports       []uint16
}

func NewZeroDocumentSenderBuilder() *zeroDocumentSenderBuilder {
	return &zeroDocumentSenderBuilder{
		inputQueues: make([]queue.MultiQueueReader, 0, 2),
		queueCounts: make([]int, 0, 2),
		ips:         make([]string, 0, 2),
		ports:       make([]uint16, 0, 2),
	}
}

func (b *zeroDocumentSenderBuilder) AddQueue(q queue.MultiQueueReader, count int) *zeroDocumentSenderBuilder {
	for _, inputQueue := range b.inputQueues {
		if &inputQueue == &q {
			return b
		}
	}
	b.inputQueues = append(b.inputQueues, q)
	b.queueCounts = append(b.queueCounts, count)
	return b
}

func (b *zeroDocumentSenderBuilder) AddZero(ip string, port uint16) *zeroDocumentSenderBuilder {
	for i := range b.ips {
		if ip == b.ips[i] && b.ports[i] == port {
			return b
		}
	}
	b.ips = append(b.ips, ip)
	b.ports = append(b.ports, port)
	return b
}

func (b *zeroDocumentSenderBuilder) Build() *ZeroDocumentSender {
	return &ZeroDocumentSender{
		inputQueues: b.inputQueues,
		queueCounts: b.queueCounts,
		ips:         b.ips,
		ports:       b.ports,
	}
}

func (s *ZeroDocumentSender) Start(queueSize int) {
	queueReaders := make([]queue.QueueReader, len(s.ips))
	queueWriters := make([]queue.QueueWriter, len(s.ips))
	for i := 0; i < len(s.ips); i++ {
		q := queue.NewOverwriteQueue(
			"6-all-doc-to-zero", queueSize,
			queue.OptionRelease(func(p interface{}) { utils.ReleaseByteBuffer(p.(*utils.ByteBuffer)) }),
			stats.OptionStatTags{"index": strconv.Itoa(i)},
		)
		queueReaders[i] = q
		queueWriters[i] = q
	}
	for i, q := range s.inputQueues {
		for key := 0; key < s.queueCounts[i]; key++ {
			go NewZeroDocumentMarshaller(q, queue.HashKey(key), queueWriters...).Start()
		}
	}
	for i := range s.ips {
		go NewZMQBytePusher(s.ips[i], s.ports[i], queueSize).QueueForward(queueReaders[i])
	}
}
