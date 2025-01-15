package dataforgebe

import (
	"dataforge-be/db"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/cors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type API struct {
	httpC *http.Client
	db    *db.DB
	nats  *nats.Conn
	kv    jetstream.KeyValue
	js    jetstream.JetStream
	os    jetstream.Stream
}

func NewAPIServer(db *db.DB, nats *nats.Conn, kv jetstream.KeyValue, js jetstream.JetStream, os jetstream.Stream) *chi.Mux {
	api := &API{
		httpC: &http.Client{},
		db:    db,
		nats:  nats,
		kv:    kv,
		js:    js,
		os:    os,
	}

	r := chi.NewRouter()

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3001"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	r.Route("/source", func(r chi.Router) {
		r.Post("/", api.createSource)
		r.Post("/id", api.getSourceById)
		r.Get("/", api.getSources)
	})

	r.Route("/destinations", func(r chi.Router) {
		r.Post("/", api.createDestination)
		r.Post("/id", api.getDestinationById)
		r.Get("/", api.getDestinations)
	})

	r.Route("/pipelines", func(r chi.Router) {
		r.Post("/", api.createPipeline)
		r.Post("/start", api.startPipeline)
	})

	return r
}
