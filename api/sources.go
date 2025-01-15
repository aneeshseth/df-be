package dataforgebe

import (
	"context"
	"dataforge-be/db/migr"
	"encoding/json"
	"net/http"
)

func (a *API) createSource(w http.ResponseWriter, r *http.Request) {
	var requestBody createSourceBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	configBytes, err := json.Marshal(requestBody.Config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = a.db.InsertSource(context.Background(), migr.CreateSourceParams{
		SourceName:        requestBody.Name,
		SourceDescription: requestBody.Description,
		Config:            configBytes,
		SourceType:        requestBody.Type,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) getSourceById(w http.ResponseWriter, r *http.Request) {
	var requestBody GetSourceByIdBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	source, err := a.db.GetSourceById(context.Background(), requestBody.Id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sourceBytes, err := json.Marshal(source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(sourceBytes)
}

func (a *API) getSources(w http.ResponseWriter, r *http.Request) {

	sources, err := a.db.GetSources(context.Background())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sourcesBytes, err := json.Marshal(sources)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(sourcesBytes)
}
