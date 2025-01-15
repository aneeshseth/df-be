package apps

import (
	"dataforge-be/nats"
	"encoding/json"
	"fmt"
	"log"

	"github.com/algolia/algoliasearch-client-go/v3/algolia/search"
)

const (
	algoliaID = "algolia"
)

type Algolia struct {
	client *search.Client
	index  *search.Index
}

func (a *Algolia) Initialize(config map[string]interface{}) error {
	appID := config["app_id"].(string)
	apiKey := config["api_key"].(string)
	indexName := config["indexName"].(string)

	client := search.NewClient(appID, apiKey)
	a.client = client
	a.index = client.InitIndex(indexName)

	fmt.Println("Algolia client and index initialized")
	return nil
}

func (a *Algolia) DestinationID() string {
	return algoliaID
}

func (a *Algolia) Run(r nats.DestinationRecord) error {
	log.Printf("Processing record with PipelineID: %d", r.PipelineID)

	for _, recordData := range r.Records {
		var document map[string]interface{}
		err := json.Unmarshal(recordData, &document)
		if err != nil {
			log.Printf("Failed to unmarshal record: %s", err)
			continue
		}

		if _, exists := document["objectID"]; !exists {
			document["objectID"] = fmt.Sprintf("%s-%d", algoliaID, r.PipelineID)
		}

		_, err = a.index.SaveObject(document)
		if err != nil {
			log.Printf("Failed to index document in Algolia: %s", err)
			continue
		}

		log.Printf("Successfully indexed document in Algolia: %v", document)
	}

	return nil
}
