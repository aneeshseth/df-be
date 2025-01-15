package destinations

import (
	"context"
	"dataforge-be/db"
	"dataforge-be/integrations"
	"dataforge-be/nats"

	"encoding/json"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

func HandleSendingToDestination(recordsToSendToDestination []byte, db *db.DB, kv jetstream.KeyValue) error {
	var destinationRecord nats.DestinationRecord
	err := json.Unmarshal(recordsToSendToDestination, &destinationRecord)
	if err != nil {
		return err
	}
	val, err := kv.Get(context.Background(), fmt.Sprintf("%s-destination", strconv.FormatInt(destinationRecord.PipelineID, 10)))
	if err != nil {
		return fmt.Errorf("failed to get value from KV store: %v", err)
	}

	valueString := string(val.Value())

	intValue, err := strconv.ParseInt(valueString, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse value from KV store: %v", err)
	}

	destination, err := db.GetDestinationById(context.Background(), intValue)
	if err != nil {
		return err
	}

	destinations := integrations.FetchDestinations()
	destinationToRun := destinations[destination.DestinationType]

	var destConfig map[string]interface{}
	err = json.Unmarshal(destination.Config, &destConfig)
	if err != nil {
		return err
	}

	destinationToRun.Initialize(destConfig)

	destinationToRun.Run(destinationRecord)

	return nil
}
