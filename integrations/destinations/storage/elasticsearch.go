package warehouses

import (
	"bytes"
	"context"
	"crypto/tls"
	"dataforge-be/nats"
	"fmt"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

const (
	elasticsearchID = "elasticsearch"
)

type ElasticSearch struct {
	client    *elasticsearch.Client
	bulkIndex esutil.BulkIndexer
	index     string
}

func (e *ElasticSearch) Initialize(config map[string]interface{}) error {

	cloudID := config["cloud_id"].(string)
	apiKey := config["api_key"].(string)
	index := config["index"].(string)

	client, bulkIndexer, index, err := initializeES(cloudID, apiKey, index)
	if err != nil {
		return fmt.Errorf("error initializing elasticsearch: %w", err)
	}

	e.client = client
	e.bulkIndex = bulkIndexer
	e.index = index

	return nil
}

func (e *ElasticSearch) DestinationID() string {
	return elasticsearchID
}

func (e *ElasticSearch) Run(record nats.DestinationRecord) error {
	ctx := context.Background()
	for _, recordBytes := range record.Records {
		err := e.bulkIndex.Add(
			ctx,
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(recordBytes),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						fmt.Printf("Error indexing document for pipeline %d: %v\n", record.PipelineID, err)
					} else {
						fmt.Printf("Elasticsearch error for pipeline %d: %s\n", record.PipelineID, res.Error.Reason)
					}
				},
				Index: e.index,
			},
		)

		if err != nil {
			return fmt.Errorf("error adding document to bulk indexer for pipeline %d: %w", record.PipelineID, err)
		}
	}

	return nil
}

func initializeES(cloudID, apiKey, index string) (*elasticsearch.Client, esutil.BulkIndexer, string, error) {
	cfg := elasticsearch.Config{
		CloudID: cloudID,
		APIKey:  apiKey,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, nil, "", fmt.Errorf("error creating elasticsearch client: %w", err)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      index,
		Client:     client,
		NumWorkers: 10,
		FlushBytes: 5e+6,
	})

	if err != nil {
		return nil, nil, "", fmt.Errorf("error creating bulk indexer: %w", err)
	}

	_, err = client.Indices.Create(index)
	if err != nil {
		return nil, nil, "", fmt.Errorf("error creating bulk indexer: %w", err)
	}

	return client, bi, index, nil
}
