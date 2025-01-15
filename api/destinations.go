package dataforgebe

import (
	"context"
	"dataforge-be/db/migr"
	"encoding/json"
	"net/http"
)

func (a *API) getDestinationById(w http.ResponseWriter, r *http.Request) {
	var requestBody GetDestinationByIdBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	destination, err := a.db.GetDestination(context.Background(), requestBody.Id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	destinationBytes, err := json.Marshal(destination)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(destinationBytes)
}

func (a *API) createDestination(w http.ResponseWriter, r *http.Request) {
	var requestBody createDestinationBody
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

	err = a.db.InsertDestination(context.Background(), migr.CreateDestinationParams{
		DestinationName:        requestBody.Name,
		DestinationDescription: requestBody.Description,
		Config:                 configBytes,
		DestinationType:        requestBody.Type,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) getDestinations(w http.ResponseWriter, _ *http.Request) {

	destinations, err := a.db.GetDestinations(context.Background())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	destinationsBytes, err := json.Marshal(destinations)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(destinationsBytes)
}
