package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/twmb/franz-go/pkg/kgo"
)

var topic = "abc"

func main() {
	ctx := context.Background()
	fmt.Println("starting...")

	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		panic(err)
	}

	client.AssignGroup("my-consumer-group", kgo.GroupTopics(topic), kgo.OnAssigned(func(ctx context.Context, data map[string][]int32) {
		fmt.Println("assigned")
		spew.Dump(data)
	}), kgo.OnRevoked(func(ctx context.Context, data map[string][]int32) {
		fmt.Println("Revoked")
		spew.Dump(data)
	}))
	defer client.Close()

consumerLoop:
	for {
		fetches := client.PollFetches(ctx)
		iter := fetches.RecordIter()

		for _, fetchErr := range fetches.Errors() {
			fmt.Printf(
				"error consuming from topic: topic=%s, partition=%d, err=%v",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err,
			)
			break consumerLoop
		}

		for !iter.Done() {
			record := iter.Next()
			fmt.Printf("consumed record with message: %v\n", string(record.Value))
		}
	}

	fmt.Println("consumer exited")

	// src := &source{}
	// p := &processor{}
	// sink := &sink{}

	// t := topology{
	// 	root: src,
	// 	rootChildren: []Processor{
	// 		p,
	// 	},
	// 	m: map[Processor][]Processor{
	// 		p: {sink},
	// 	},
	// }

	// // One iteration = take one record, and pass it through whole topology.
	// for {
	// 	fmt.Println("abc")
	// 	s := t.root

	// 	kv, err := s.Poll()
	// 	if err != nil {
	// 		break
	// 	}

	// 	nextNodes := t.rootChildren

	// 	spew.Dump(nextNodes)

	// 	// One iteration = one process one node's children
	// 	for _, node := range nextNodes {
	// 		t.step(node, kv)
	// 	}

	// }

	fmt.Println("ENDE")
}

func (t *topology) step(p Processor, kv *KeyValue) {
	kvOut, _ := p.Process(kv)

	children := t.getChildren(p)

	for _, child := range children {
		t.step(child, kvOut)
	}
}

type topology struct {
	root         *source
	rootChildren []Processor
	m            map[Processor][]Processor
}

func (t *topology) getChildren(node Processor) []Processor {
	return t.m[node]
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type source struct{}

func (s *source) Poll() (kv *KeyValue, err error) {
	time.Sleep(time.Second * 5)
	return &KeyValue{[]byte("key"), []byte("value")}, nil
}

type processor struct{}

func (p *processor) Process(kv *KeyValue) (kvOut *KeyValue, err error) {
	fmt.Println("Processing ", string(kv.Key), ":", string(kv.Value))

	return &KeyValue{kv.Key, []byte("processed value")}, nil
}

func (p *processor) Flush() error {
	return nil
}

type sink struct{}

func (p *sink) Process(kv *KeyValue) (kvOut *KeyValue, err error) {
	fmt.Println("SINK - Produce to kafka ", string(kv.Key), ":", string(kv.Value))

	return nil, nil
}

func (p *sink) Flush() error {
	return nil
}

// ------

// To be impl by the user
type Processor interface {
	Process(kv *KeyValue) (kvOut *KeyValue, err error) // For now just outputting one item .. later: multiple possible.
	Flush() error
}

type StateStore interface {
	Set(key, value []byte) error
	Get(key []byte) (value []byte, err error)
}

type Serializable interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

// TODO distinguish between processor and sink (sink=terminal)
