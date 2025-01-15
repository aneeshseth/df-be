package apps

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	mongoID        = "mongodb"
	baseURL        = "https://cloud.mongodb.com/api/atlas/v2"
	projEventsPath = "/groups/%s/events"
	projsPath      = "/groups"
)

type MongoDB struct {
	client *Client
	state  *InputState
}

func (m *MongoDB) Initialize(config map[string]interface{}) error {
	clientID, _ := config["client_id"].(string)
	clientSecret, _ := config["client_secret"].(string)

	authenticator := NewTokenAuthenticator(clientID, clientSecret, &http.Client{
		Timeout: 10 * time.Second,
	})

	m.client = NewClient(authenticator, &http.Client{
		Timeout: 10 * time.Second,
	})

	return nil
}

func (m *MongoDB) SourceID() string {
	return mongoID
}

func (m *MongoDB) Run(ctx context.Context, pipelineID int64, js jetstream.JetStream) error {
	currentSyncTime := time.Now().UTC()

	projIds, err := m.client.GetProjsIds(ctx)
	if err != nil {
		return fmt.Errorf("failed to get project IDs: %w", err)
	}

	if m.state == nil {
		m.state = &InputState{
			LastSyncTime: currentSyncTime.Add(-10000 * time.Hour),
		}
	}

	for _, id := range projIds {
		pageNumber := 1
		for {
			events, err := m.client.GetProjEventsByPage(ctx, id, m.state.LastSyncTime, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to get events for project %s: %w", id, err)
			}

			if len(events.Results) == 0 {
				break
			}

			var allEvents [][]byte
			for _, event := range events.Results {
				eventBytes, err := json.Marshal(event)
				if err != nil {
					return fmt.Errorf("failed to marshal event: %w", err)
				}
				allEvents = append(allEvents, eventBytes)
			}

			destRecord := map[string]interface{}{
				"pipeline_id": pipelineID,
				"records":     allEvents,
			}
			destRecordBytes, err := json.Marshal(destRecord)
			if err != nil {
				return fmt.Errorf("failed to marshal event: %w", err)
			}
			ack, err := js.Publish(ctx, "OUTPUT", destRecordBytes)
			if err != nil {
				return fmt.Errorf("failed to publish message to OUTPUT subject: %w", err)
			}

			log.Printf("Message published to OUTPUT subject. Ack: Stream=%s, Seq=%d", ack.Stream, ack.Sequence)
			pageNumber++
		}
	}

	m.state.LastSyncTime = currentSyncTime
	return nil
}

type TokenAuthenticator struct {
	clientID     string
	clientSecret string
	client       *http.Client
	mu           sync.RWMutex
	tokenInfo    *TokenInfo
}

type TokenInfo struct {
	Token     string
	ExpiresAt time.Time
}

func NewTokenAuthenticator(clientID, clientSecret string, client *http.Client) *TokenAuthenticator {
	return &TokenAuthenticator{
		clientID:     clientID,
		clientSecret: clientSecret,
		client:       client,
	}
}

func (t *TokenAuthenticator) AddAuth(req *http.Request) error {
	token, err := t.getValidToken()
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	return nil
}

func (t *TokenAuthenticator) getValidToken() (string, error) {
	t.mu.RLock()
	if t.tokenInfo != nil && time.Now().Before(t.tokenInfo.ExpiresAt) {
		token := t.tokenInfo.Token
		t.mu.RUnlock()
		return token, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tokenInfo != nil && time.Now().Before(t.tokenInfo.ExpiresAt) {
		return t.tokenInfo.Token, nil
	}

	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", t.clientID, t.clientSecret)))
	data := url.Values{}
	data.Set("grant_type", "client_credentials")

	req, err := http.NewRequest("POST", "https://cloud.mongodb.com/api/oauth/token", bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", auth))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	t.tokenInfo = &TokenInfo{
		Token:     tokenResp.AccessToken,
		ExpiresAt: time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
	}

	return tokenResp.AccessToken, nil
}

type Client struct {
	client        *http.Client
	authenticator *TokenAuthenticator
}

type MongoDBOrgEventResponse struct {
	Results []map[string]interface{} `json:"results"`
}

type mongoDbProjsResponse struct {
	Results []mongoDbProjResult `json:"results"`
}

type mongoDbProjResult struct {
	ID string `json:"id"`
}

type MongoDBOrgsResponse struct {
	Results []struct {
		ID string `json:"id"`
	} `json:"results"`
}

type MongoDBProjsResponse struct {
	Results []struct {
		ID string `json:"id"`
	} `json:"results"`
}

type InputState struct {
	LastSyncTime time.Time `json:"last_sync_time"`
}

func NewClient(authenticator *TokenAuthenticator, client *http.Client) *Client {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		client:        client,
		authenticator: authenticator,
	}
}

func (c *Client) doRequest(req *http.Request) ([]byte, error) {
	if err := c.authenticator.AddAuth(req); err != nil {
		return nil, fmt.Errorf("authentication error: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func (c *Client) GetProjEventsByPage(ctx context.Context, projID string, lastRunTime time.Time, pageNumber int) (*MongoDBOrgEventResponse, error) {
	path := fmt.Sprintf("%s%s", baseURL, fmt.Sprintf(projEventsPath, projID))

	queryParams := url.Values{}
	queryParams.Add("minDate", lastRunTime.Format(time.RFC3339))
	queryParams.Add("pageNum", fmt.Sprintf("%d", pageNumber))

	pathWithQuery := fmt.Sprintf("%s?%s", path, queryParams.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", pathWithQuery, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.atlas.2024-08-05+json")

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	eventsResponse := &MongoDBOrgEventResponse{}
	if err := json.Unmarshal(resp, eventsResponse); err != nil {
		return nil, err
	}

	return eventsResponse, nil
}

func (c *Client) GetProjsIds(ctx context.Context) ([]string, error) {
	var Ids []string
	path := fmt.Sprintf("%s%s", baseURL, projsPath)
	req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.atlas.2024-08-05+json")

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}
	projsResponse := &mongoDbProjsResponse{}
	if err := json.Unmarshal(resp, projsResponse); err != nil {
		return nil, err
	}
	for _, org := range projsResponse.Results {
		Ids = append(Ids, org.ID)
	}
	return Ids, nil
}
