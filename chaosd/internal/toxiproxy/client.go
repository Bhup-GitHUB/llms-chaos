package toxiproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	if baseURL == "" {
		baseURL = "http://localhost:8474"
	}
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func (c *Client) Populate(proxies []map[string]any) error {
	raw, _ := json.Marshal(proxies)
	resp, err := c.HTTP.Post(c.BaseURL+"/populate", "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("populate status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) AddToxic(proxy, name, toxicType, stream string, toxicity float64, attrs map[string]any) error {
	payload := map[string]any{
		"name": name, "type": toxicType, "stream": stream,
		"toxicity": toxicity, "attributes": attrs,
	}
	raw, _ := json.Marshal(payload)
	resp, err := c.HTTP.Post(c.BaseURL+"/proxies/"+proxy+"/toxics", "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("add toxic status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) RemoveToxic(proxy, name string) error {
	req, _ := http.NewRequest("DELETE", c.BaseURL+"/proxies/"+proxy+"/toxics/"+name, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *Client) Toggle(proxy string, enabled bool) error {
	raw, _ := json.Marshal(map[string]any{"enabled": enabled})
	req, _ := http.NewRequest("POST", c.BaseURL+"/proxies/"+proxy, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("toggle status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Reset() error {
	resp, err := c.HTTP.Post(c.BaseURL+"/reset", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
