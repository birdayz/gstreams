package gstreams

import "fmt"

// One producer per Task. They may use the same kafka clients.
type Producer struct {
}

func (p *Producer) Send(key, value []byte, topic string, partition uint32) error { // TODO headers, stream partitioner/partition number?
	fmt.Println("Send", string(key), string(value))
	// Serialize
	return nil
}

func (p *Producer) Flush() error {
	return nil
}
