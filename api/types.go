package dataforgebe

import (
	"encoding/json"
	"math/rand"
	"time"
)

type createSourceBody struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Config      map[string]interface{} `json:"config"`
	Type        string                 `json:"source_type"`
}

type createTransformationBody struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"transformation_type"`
}

type createDestinationBody struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Config      map[string]interface{} `json:"config"`
	Type        string                 `json:"destination_type"`
}

type GetDestinationByIdBody struct {
	Id int64 `json:"id"`
}

type GetSourceByIdBody struct {
	Id int64 `json:"id"`
}

type startPipelineBody struct {
	PipelineID int64 `json:"pipeline_id"`
}

type createPipelineBody struct {
	SourceID      int64 `json:"source_id"`
	DestinationID int64 `json:"destination_id"`
}

type inputEventsBody struct {
	PipelineID int64             `json:"pipeline_id"`
	Records    []json.RawMessage `json:"records"`
}

func generateRandomBigInt() int64 {
	rand.Seed(time.Now().UnixNano())

	return rand.Int63()
}
