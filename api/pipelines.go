package dataforgebe

import (
	"context"
	"dataforge-be/db/migr"
	i "dataforge-be/integrations"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func (a *API) startPipeline(w http.ResponseWriter, r *http.Request) {
	var requestBody startPipelineBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pipeline, err := a.db.GetPipelineById(context.Background(), requestBody.PipelineID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	source, err := a.db.GetSourceById(context.Background(), pipeline.SourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var sourceConfig map[string]interface{}
	err = json.Unmarshal(source.Config, &sourceConfig)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sources := i.FetchSources()
	sourceToStart := sources[source.SourceType]
	err = sourceToStart.Initialize(sourceConfig)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = sourceToStart.Run(context.Background(), pipeline.ID, a.js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) createPipeline(w http.ResponseWriter, r *http.Request) {
	fmt.Println("in create pipeline")
	var requestBody createPipelineBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("here")
	err = a.db.InsertPipeline(context.Background(), migr.CreatePipelineParams{
		SourceID:      requestBody.SourceID,
		DestinationID: requestBody.DestinationID,
	})
	fmt.Println(err)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pipelines, err := a.db.GetPipelines(context.Background())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var insertedPipeline migr.Pipeline
	for _, pipeline := range pipelines {
		if pipeline.SourceID == requestBody.SourceID && pipeline.DestinationID == requestBody.DestinationID {
			insertedPipeline = pipeline
			break
		}
	}

	_, err = a.kv.Put(context.Background(), fmt.Sprintf("%s-source", strconv.FormatInt(insertedPipeline.ID, 10)), []byte{byte(requestBody.SourceID)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = a.kv.Put(
		context.Background(),
		fmt.Sprintf("%s-destination", strconv.FormatInt(insertedPipeline.ID, 10)),
		[]byte(strconv.FormatInt(requestBody.DestinationID, 10)),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
