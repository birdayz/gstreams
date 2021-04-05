package gstreams

import (
	"context"
	"fmt"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	stateCreated = iota
	stateStarting
	statePartitionsRevoked
	statePartitionsAssigned
	stateRunning
	statePendingShutdown
	statePending
)

type StreamThread struct {
	tasks map[string]map[int32]*Task
	m     sync.Mutex

	consumer *kgo.Client
}

func NewStreamThread(group string, topics ...string) *StreamThread {
	client, _ := kgo.NewClient(kgo.SeedBrokers([]string{"localhost:9092"}...))
	st := &StreamThread{
		tasks:    make(map[string]map[int32]*Task),
		consumer: client,
	}

	client.AssignGroup(group, kgo.DisableAutoCommit(), kgo.GroupTopics(topics...), kgo.OnAssigned(func(ctx context.Context, topicPartitions map[string][]int32) {
		st.m.Lock()
		defer st.m.Unlock()

		for topic, partitions := range topicPartitions {

			if _, ok := st.tasks[topic]; !ok {
				st.tasks[topic] = make(map[int32]*Task)

				for _, p := range partitions {

					// TODO - need TOPOLOGY here!

					st.tasks[topic][p] = NewTask(client)
				}
			}
		}

		fmt.Println("assigned", topicPartitions)
	}), kgo.OnRevoked(func(ctx context.Context, data map[string][]int32) {
		// Commit task
		// Close task
		fmt.Println("Revoked")
		spew.Dump(data)
	}))

	return st
}

func (st *StreamThread) Start() {
	go st.run()
}

func (st *StreamThread) run() {
	for {

		// 1. Fetch records from client
		fetches := st.consumer.PollFetches(context.TODO())

		iter := fetches.RecordIter()
		for _, fetchErr := range fetches.Errors() {
			fmt.Printf(
				"error consuming from topic: topic=%s, partition=%d, err=%v",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err,
			)
		}

		// 2. Add records to corresponding tasks
		for !iter.Done() {
			record := iter.Next()
			fmt.Printf("consumed record with message: %v\n", string(record.Value))

			if x, ok := st.tasks[record.Topic]; ok {
				if m, ok := x[record.Partition]; ok {
					m.AddRecords([]*kgo.Record{record})
				}
			}
		}

		// 3. Process tasks
		for _, partition := range st.tasks {
			for _, task := range partition {
				task.Process()
			}
		}

		// 4. Commit

		// 4.1 Flush tasks -> individual nodes

		// 4.2 Do actual commit
		for _, partition := range st.tasks {
			for _, task := range partition {
				_ = task.Commit()
			}
		}
	}
}
