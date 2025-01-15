package integrations

import (
	"context"
	"dataforge-be/integrations/destinations/apps"
	storage "dataforge-be/integrations/destinations/storage"
	app_sources "dataforge-be/integrations/sources/apps"
	warehouse_sources "dataforge-be/integrations/sources/warehouses"
	"dataforge-be/nats"

	"github.com/nats-io/nats.go/jetstream"
)

type Source interface {
	Initialize(config map[string]interface{}) error
	SourceID() string
	Run(ctx context.Context, pipelineID int64, os jetstream.JetStream) error
}

type Destination interface {
	Initialize(config map[string]interface{}) error
	DestinationID() string
	Run(d nats.DestinationRecord) error
}

func FetchSources() map[string]Source {
	return map[string]Source{
		"snowflake": &warehouse_sources.Snowflake{},
		"mongodb":   &app_sources.MongoDB{},
	}
}

func FetchDestinations() map[string]Destination {
	return map[string]Destination{
		"algolia":       &apps.Algolia{},
		"elasticsearch": &storage.ElasticSearch{},
	}
}
