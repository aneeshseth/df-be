-- name: GetSourceById :one
SELECT * FROM sources
WHERE id = ?;

-- name: GetDestinationById :one
SELECT * FROM destinations
WHERE id = ?;

-- name: GetPipelineById :one
SELECT * FROM pipelines
WHERE id = ?;

-- name: GetAllSources :many
SELECT * FROM sources;

-- name: GetAllDestinations :many
SELECT * FROM destinations;

-- name: GetAllPipelines :many
SELECT * FROM pipelines;

-- name: CreateSource :exec
INSERT INTO sources (source_name, source_type, source_description, config)
VALUES (?, ?, ?, ?);

-- name: CreatePipeline :exec
INSERT INTO pipelines (source_id, destination_id)
VALUES (?, ?);

-- name: CreateDestination :exec
INSERT INTO destinations (destination_name, destination_type, destination_description, config)
VALUES (?, ?, ?, ?);

