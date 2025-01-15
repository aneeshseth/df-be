package sources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	snowflakeID = "snowflake"
)

type Snowflake struct {
	conn        *sql.DB
	isStreaming bool
	DbName      string
	WHName      string
	js          jetstream.JetStream
}

func (s *Snowflake) Initialize(config map[string]interface{}) error {
	snowflakeUsername := config["username"].(string)
	snowflakePassword := config["password"].(string)
	snowflakeAcc := config["acc"].(string)
	snowflakeOrg := config["org"].(string)
	snowflakeDB := config["db"].(string)
	snowflakeWH := config["wh"].(string)
	snowflakeIsStream := config["stream"].(bool)
	snowflakeURL := fmt.Sprintf("%s:%s@%s-%s/%s?warehouse=%s", snowflakeUsername, snowflakePassword, snowflakeAcc, snowflakeOrg, snowflakeDB, snowflakeWH)
	db, err := sql.Open("snowflake", snowflakeURL)
	if err != nil {
		panic(err)
	}
	s.conn = db
	s.isStreaming = snowflakeIsStream
	s.DbName = snowflakeDB
	s.WHName = snowflakeWH
	return nil
}

func (s *Snowflake) SourceID() string {
	return snowflakeID
}

func (s *Snowflake) Run(ctx context.Context, pipelineID int64, js jetstream.JetStream) error {
	s.js = js
	var err error
	if s.isStreaming {
		err = s.HandleInWarehouseDiffing()
		if err != nil {
			return err
		}
		time.Sleep(time.Minute * 2)
		err = s.HandleStreaming(ctx, pipelineID)
		if err != nil {
			return err
		}
	}
	return err
}

func sendBatch(pipelineID int64, batch [][]byte, js jetstream.JetStream, ctx context.Context) error {
	requestBody := map[string]interface{}{
		"pipeline_id": pipelineID,
		"records":     batch,
	}

	destRecordBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %v", err)
	}
	ack, err := js.Publish(ctx, "OUTPUT", destRecordBytes)
	if err != nil {
		return fmt.Errorf("failed to publish message to OUTPUT subject: %w", err)
	}
	log.Printf("Message published to OUTPUT subject. Ack: Stream=%s, Seq=%d", ack.Stream, ack.Sequence)
	return nil
}

func (s *Snowflake) HandleStreaming(ctx context.Context, pipelineID int64) error {
	tables, err := s.fetchTablesInDB()
	if err != nil {
		return fmt.Errorf("failed to fetch tables: %v", err)
	}

	for _, tableName := range tables {
		if s.isDynamicTable(tableName) {
			fmt.Printf("Skipping dynamic table: %s\n", tableName)
			continue
		}
		streamName := fmt.Sprintf("%s.PUBLIC.%s_STREAM", s.DbName, tableName)
		query := fmt.Sprintf("SELECT * FROM %s", streamName)
		rows, err := s.conn.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query stream %s: %v", streamName, err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns for stream %s: %v", streamName, err)
		}

		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var batchSize = 100
		var batch [][]byte

		for rows.Next() {
			if err := rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("failed to scan row: %v", err)
			}

			record := make(map[string]interface{})
			for i, col := range columns {
				val := values[i]
				record[col] = val
			}

			jsonData, err := json.Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to marshal record: %v", err)
			}

			batch = append(batch, jsonData)

			if len(batch) >= batchSize {
				if err := sendBatch(pipelineID, batch, s.js, ctx); err != nil {
					return err
				}
				batch = nil
			}
		}

		if len(batch) > 0 {
			if err := sendBatch(pipelineID, batch, s.js, ctx); err != nil {
				return err
			}
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("error during row iteration: %v", err)
		}

		err = s.handleStreamDeletionAndRecreation()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Snowflake) handleStreamDeletionAndRecreation() error {
	tables, err := s.fetchTablesInDB()
	if err != nil {
		return err
	}

	deleteStreamQuery := `DROP STREAM %s;`
	streamOnDynamicTableQuery := `CREATE OR REPLACE STREAM %s ON DYNAMIC TABLE %s;`

	fmt.Println("Fetched Tables:", tables)

	for _, tableName := range tables {
		if s.isDynamicTable(tableName) {
			fmt.Printf("Skipping dynamic table: %s\n", tableName)
			continue
		}

		deleteQuery := fmt.Sprintf(deleteStreamQuery, fmt.Sprintf("%s.PUBLIC.%s_STREAM", s.DbName, tableName))
		fmt.Println("Executing:", deleteQuery)
		_, err := s.conn.Exec(deleteQuery)
		if err != nil {
			return fmt.Errorf("failed to delete stream: %v", err)
		}

		streamQuery := fmt.Sprintf(streamOnDynamicTableQuery, fmt.Sprintf("%s.PUBLIC.%s", s.DbName, fmt.Sprintf("%s_STREAM", tableName)), fmt.Sprintf("%s.PUBLIC.%s", s.DbName, fmt.Sprintf("%s_DYNAMIC", tableName)))
		fmt.Println("Executing:", streamQuery)
		_, err = s.conn.Exec(streamQuery)
		if err != nil {
			return fmt.Errorf("failed to create stream: %v", err)
		}
	}

	return nil
}

func (s *Snowflake) isDynamicTable(tableName string) bool {
	return strings.Contains(tableName, "_DYNAMIC")
}

func (s *Snowflake) fetchTablesInDB() ([]string, error) {
	_, err := s.conn.Exec("USE INITDB")
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Error getting columns:", err)
	}
	var tableNames []string

	for rows.Next() {
		columnsData := make([]interface{}, len(columns))
		for i := range columnsData {
			columnsData[i] = new(interface{})
		}

		err := rows.Scan(columnsData...)
		if err != nil {
			return nil, err
		}

		for i, col := range columnsData {
			if columns[i] == "name" {
				tableName, ok := (*(col.(*interface{}))).(string)
				if ok {
					tableNames = append(tableNames, tableName)
				} else {
					return nil, errors.New("error type assertion to string")
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error with row iteration:", err)
	}

	return tableNames, nil
}

func (s *Snowflake) HandleInWarehouseDiffing() error {
	tables, err := s.fetchTablesInDB()
	if err != nil {
		return err
	}

	dynamicTableCreationQuery := `CREATE OR REPLACE DYNAMIC TABLE %s
	TARGET_LAG = '1 minutes'
	WAREHOUSE = %s
	REFRESH_MODE = auto
	INITIALIZE = on_create
	AS
	  SELECT * FROM %s;`

	streamOnDynamicTableQuery := `CREATE OR REPLACE STREAM %s ON DYNAMIC TABLE %s;`
	for _, i := range tables {
		query := fmt.Sprintf(dynamicTableCreationQuery, fmt.Sprintf("%s_DYNAMIC", i), s.WHName, fmt.Sprintf("%s.PUBLIC.%s", s.DbName, i))
		_, err := s.conn.Exec(query)
		if err != nil {
			return err
		}

		streamQuery := fmt.Sprintf(streamOnDynamicTableQuery, fmt.Sprintf("%s.PUBLIC.%s", s.DbName, fmt.Sprintf("%s_STREAM", i)), fmt.Sprintf("%s.PUBLIC.%s", s.DbName, fmt.Sprintf("%s_DYNAMIC", i)))
		_, err = s.conn.Exec(streamQuery)
		if err != nil {
			return err
		}
	}

	return nil
}
