package dashboard

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"wavenet/internal/model"
)

// Client sends node events to the optional dashboard server.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
}

func NewClient(baseURL string, logger *log.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		logger: logger,
	}
}

func (c *Client) Send(event model.DashboardEvent) {
	if c == nil || c.baseURL == "" {
		return
	}

	body, err := json.Marshal(event)
	if err != nil {
		return
	}

	resp, err := c.httpClient.Post(c.baseURL+"/events", "application/json", bytes.NewReader(body))
	if err != nil {
		if c.logger != nil {
			c.logger.Printf("[dashboard] event post failed: %v", err)
		}
		return
	}
	defer resp.Body.Close()
}
