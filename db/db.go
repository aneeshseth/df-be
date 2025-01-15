package db

import (
	"context"
	"database/sql"
	"dataforge-be/db/migr"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	db   *sql.DB
	migr *migr.Queries
}

func NewDB(sqlUser string, sqlPass string, globalDB string) (*DB, error) {
	dbConnURL := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s?parseTime=true", sqlUser, sqlPass, globalDB)
	db, err := sql.Open("mysql", dbConnURL)
	if err != nil {
		return nil, err
	}

	queries := migr.New(db)

	dbType := &DB{
		db:   db,
		migr: queries,
	}

	return dbType, nil
}

func (d *DB) InsertSource(ctx context.Context, params migr.CreateSourceParams) error {
	return d.migr.CreateSource(ctx, params)
}

func (d *DB) InsertDestination(ctx context.Context, params migr.CreateDestinationParams) error {
	return d.migr.CreateDestination(ctx, params)
}

func (d *DB) InsertPipeline(ctx context.Context, params migr.CreatePipelineParams) error {
	return d.migr.CreatePipeline(ctx, params)
}

func (d *DB) GetDestinations(ctx context.Context) ([]migr.Destination, error) {
	return d.migr.GetAllDestinations(ctx)
}

func (d *DB) GetPipelines(ctx context.Context) ([]migr.Pipeline, error) {
	return d.migr.GetAllPipelines(ctx)
}

func (d *DB) GetSources(ctx context.Context) ([]migr.Source, error) {
	return d.migr.GetAllSources(ctx)
}

func (d *DB) GetSourceById(ctx context.Context, id int64) (migr.Source, error) {
	return d.migr.GetSourceById(ctx, id)
}

func (d *DB) GetDestinationById(ctx context.Context, id int64) (migr.Destination, error) {
	return d.migr.GetDestinationById(ctx, id)
}

func (d *DB) GetDestination(ctx context.Context, id int64) (migr.Destination, error) {
	return d.migr.GetDestinationById(ctx, id)
}

func (d *DB) GetPipelineById(ctx context.Context, id int64) (migr.Pipeline, error) {
	return d.migr.GetPipelineById(ctx, id)
}
