package nats

import "github.com/nats-io/nats.go"

func NewNatsServer(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

type DestinationRecord struct {
	PipelineID int64    `json:"pipeline_id"`
	Records    [][]byte `json:"records"`
}
