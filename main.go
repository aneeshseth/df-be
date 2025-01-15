package main

import (
	"context"
	dataforgebe "dataforge-be/api"
	"dataforge-be/db"
	"dataforge-be/integrations/destinations"
	n "dataforge-be/nats"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Dataforge interface {
	DBService() error
	Nats() (*nats.Conn, error)
	NatsJS(nc *nats.Conn) error
	APIService() *chi.Mux
}

type DataforgeService struct {
	db       *db.DB
	natsConn *nats.Conn
	kv       jetstream.KeyValue
	os       jetstream.Stream
	js       jetstream.JetStream
}

func (d *DataforgeService) DBService() error {
	db, err := db.NewDB(os.Getenv("SQL_USER"), os.Getenv("SQL_PASS"), os.Getenv("GLOBAL_DB"))
	if err != nil {
		log.Fatal(err)
		return err
	}
	d.db = db
	log.Println("DB connection initialized")
	return nil
}

func (d *DataforgeService) Nats() (*nats.Conn, error) {
	conn, err := n.NewNatsServer(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	d.natsConn = conn
	log.Println("NATS connection initialized")
	return conn, nil
}

func (d *DataforgeService) NatsJS(nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to initialize JetStream: ", err)
		return err
	}
	d.js = js

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "dataforge",
	})

	if err != nil {
		log.Fatal("Failed to create KeyValue store: ", err)
		return err
	}
	d.kv = kv

	os, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "OUTPUTS",
		Subjects: []string{"OUTPUT"},
	})
	if err != nil {
		log.Fatal("Failed to create Outputs stream: ", err)
		return err
	}
	d.os = os

	log.Println("NATS KV, and Output stream initialized")
	return nil
}

func (d *DataforgeService) APIService() *chi.Mux {
	return dataforgebe.NewAPIServer(d.db, d.natsConn, d.kv, d.js, d.os)
}

func RunApp(d Dataforge) *chi.Mux {
	err := d.DBService()
	if err != nil {
		panic(err)
	}
	conn, err := d.Nats()
	if err != nil {
		panic(err)
	}
	err = d.NatsJS(conn)
	if err != nil {
		panic(err)
	}
	return d.APIService()
}

func main() {
	df := &DataforgeService{}

	server := RunApp(df)

	destinationsConsumer, err := df.os.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("Failed to create or update consumer: %v", err)
	}

	_, err = destinationsConsumer.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		err := destinations.HandleSendingToDestination(msg.Data(), df.db, df.kv)
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		log.Fatalf("Failed to consume messages: %v", err)
	}

	if err := http.ListenAndServe(":3000", server); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
