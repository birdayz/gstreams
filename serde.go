package gstreams

type Serializer interface {
	Serialize(interface{}) ([]byte, error)
}

type Deserializer interface {
	Deserialize([]byte) (interface{}, error)
}

type Serde interface {
	Serializer
	Deserializer
}
