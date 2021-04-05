package gstreams

import (
	"errors"
	"fmt"
)

// -- Processor

var _ = Processor(&ProcessorNode{})

type ProcessorNode struct {
	name      string
	processor Processor
	childMap  map[string]*ProcessorNode
}

func (pn *ProcessorNode) Process(ctx *ProcessorContext, record *KeyValue) (err error) {
	return pn.processor.Process(ctx, record)
}

func (pn *ProcessorNode) addChild(processorName string, node *ProcessorNode) error {
	if _, ok := pn.childMap[processorName]; !ok {
		pn.childMap[processorName] = node
	}
	return errors.New("child already exists")
}

func (pn *ProcessorNode) Children() map[string]*ProcessorNode {
	return pn.childMap
}

func (pn *ProcessorNode) Child(name string) *ProcessorNode {
	return pn.childMap[name]
}

func (pn *ProcessorNode) Flush() error { return pn.processor.Flush() }

// -- Source --> call deserializer

// Source node does NOT impl. Processor
type SourceNode struct {
}

func (sn *SourceNode) Commit(offsets map[uint32]uint64) error {
	// TODO Track offsets safe to be committed
	return nil
}

// -- Sink --> call serializer

var _ = Processor(&SinkNode{})

type SinkNode struct {
	producer *Producer

	keySerializer   Serializer
	valueSerializer Serializer

	topic string
}

func (sn *SinkNode) Process(ctx *ProcessorContext, record *KeyValue) (err error) {
	keyBytes, err := sn.keySerializer.Serialize(record.Key)
	if err != nil {
		return fmt.Errorf("failed to serialize key: %w", err)
	}

	valueBytes, errValue := sn.valueSerializer.Serialize(record.Value)
	if errValue != nil {
		return fmt.Errorf("failed to serialize value: %w", err)
	}

	return sn.producer.Send(keyBytes, valueBytes, sn.topic, ctx.Partition())
}

func (sn *SinkNode) Flush() error {
	return nil
}
