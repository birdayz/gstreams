package gstreams

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Task = One specific partition of one specific topic
type Task struct {
	sourceNode *SourceNode

	committableOffsets map[string]map[int32]kgo.EpochOffset

	client *kgo.Client

	backlog []*kgo.Record
}

func NewTask(client *kgo.Client) *Task {
	return &Task{
		backlog:            make([]*kgo.Record, 0, 10),
		committableOffsets: make(map[string]map[int32]kgo.EpochOffset),
		client:             client,
	}
}

func (t *Task) AddRecords(records []*kgo.Record) {
	t.backlog = append(t.backlog, records...)
}

func (t *Task) Process() (err error) {
	fmt.Println("Process")

	for _, record := range t.backlog {
		fmt.Println(string(record.Key), string(record.Value), record.Offset)

		// TODO - call processor nodes..

		if _, ok := t.committableOffsets[record.Topic]; !ok {
			t.committableOffsets[record.Topic] = make(map[int32]kgo.EpochOffset)
		}

		t.committableOffsets[record.Topic][record.Partition] = kgo.EpochOffset{Offset: record.Offset + 1}
	}

	t.backlog = t.backlog[:0]

	return nil
}

func (t *Task) Commit() error {
	t.client.BlockingCommitOffsets(context.TODO(), t.committableOffsets, func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error) {
		fmt.Println("Commit", t.committableOffsets)
	})
	return nil
}
