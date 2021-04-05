package gstreams

import "context"

type Processor interface {
	Process(context *ProcessorContext, record *KeyValue) (err error) // For now just outputting one item .. later: multiple possible.
	Flush() error
}

type ProcessorContext struct {
	context.Context

	partition uint32
	offset    uint64
	topic     string
	headers   map[string]string

	results map[string][]*KeyValue
}

// TODO how to set headers? etc
func (pc *ProcessorContext) Forward(kv *KeyValue, to ...string) {
	if len(to) == 0 {
		for k := range pc.results {
			pc.results[k] = append(pc.results[k], kv)
		}
	} else {
		for _, t := range to {
			pc.results[t] = append(pc.results[t], kv)
		}
	}
}

func (pc *ProcessorContext) Partition() uint32 {
	return pc.partition
}

func (pc *ProcessorContext) Offset() uint64 {
	return pc.offset
}

func (pc *ProcessorContext) Topic() string {
	return pc.topic
}

func (pc *ProcessorContext) Header(key string) string {
	return pc.headers[key]
}
