package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bhupesh/llms-chaos/gateway/internal/breaker"
	"github.com/bhupesh/llms-chaos/gateway/internal/middleware"
	"github.com/bhupesh/llms-chaos/gateway/internal/router"
)

type Forwarder struct {
	Pool    *Pool
	Breakers *breaker.Registry
	Metrics *middleware.Metrics
	DialTimeout   time.Duration
	HeaderTimeout time.Duration
}

type Pool interface {
	Pick() *router.Worker
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
	Stream      bool          `json:"stream"`
}

type workerFrame struct {
	Token        string `json:"token"`
	Index        int    `json:"index"`
	FinishReason string `json:"finish_reason"`
	RequestID    string `json:"request_id"`
}

func (f *Forwarder) Serve(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.MaxTokens <= 0 {
		req.MaxTokens = 64
	}
	requestID := w.Header().Get("X-Request-ID")
	workerReq := map[string]any{
		"request_id":  requestID,
		"messages":    req.Messages,
		"model":       req.Model,
		"max_tokens":  req.MaxTokens,
		"temperature": req.Temperature,
	}
	body, _ := json.Marshal(workerReq)

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		wrk := f.Pool.Pick()
		if wrk == nil {
			http.Error(w, "no workers available", http.StatusServiceUnavailable)
			return
		}
		b := f.Breakers.For(wrk.ID)
		if !b.Allow() {
			continue
		}
		if attempt == 1 {
			f.Metrics.Retry()
		}
		committed := f.forward(wrk, body, requestID, req, w, r)
		b.Record(committed.ok)
		if committed.ok || committed.sentHeaders {
			return
		}
		lastErr = committed.err
	}
	if lastErr != nil {
		http.Error(w, "upstream failed", http.StatusBadGateway)
		return
	}
	http.Error(w, "upstream failed", http.StatusBadGateway)
}

type outcome struct {
	ok          bool
	sentHeaders bool
	err         error
}

func (f *Forwarder) forward(wrk *router.Worker, body []byte, requestID string, req ChatRequest, w http.ResponseWriter, r *http.Request) outcome {
	addr := fmt.Sprintf("%s:%d", wrk.Host, wrk.Port)
	dialer := &net.Dialer{Timeout: f.DialTimeout}
	conn, err := dialer.DialContext(r.Context(), "tcp", addr)
	if err != nil {
		return outcome{err: err}
	}
	defer conn.Close()
	deadline := time.Now().Add(f.HeaderTimeout + 120*time.Second)
	_ = conn.SetDeadline(deadline)

	fmt.Fprintf(conn, "POST /generate HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", addr, len(body))
	if _, err := conn.Write(body); err != nil {
		return outcome{err: err}
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		return outcome{err: err}
	}
	if !strings.Contains(status, "200") {
		return outcome{err: fmt.Errorf("worker status: %s", strings.TrimSpace(status))}
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return outcome{err: err}
		}
		if line == "\r\n" {
			break
		}
	}

	start := time.Now()
	firstToken := true
	flusher, _ := w.(http.Flusher)

	var collected strings.Builder
	headerSent := false
	sendChunk := func(data string) {
		if !headerSent {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			headerSent = true
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
	}

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if headerSent {
				return outcome{ok: false, sentHeaders: true, err: err}
			}
			return outcome{err: err}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			if !req.Stream {
				resp := map[string]any{
					"id":      requestID,
					"object":  "chat.completion",
					"model":   req.Model,
					"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": collected.String()}, "finish_reason": "stop"}},
				}
				raw, _ := json.Marshal(resp)
				w.Header().Set("Content-Type", "application/json")
				w.Write(raw)
				f.Metrics.End(true)
				return outcome{ok: true, sentHeaders: true}
			}
			sendChunk("[DONE]")
			f.Metrics.End(true)
			return outcome{ok: true, sentHeaders: true}
		}
		var frame workerFrame
		if err := json.Unmarshal([]byte(payload), &frame); err != nil {
			continue
		}
		if firstToken {
			f.Metrics.ObserveTTFT(time.Since(start).Seconds())
			firstToken = false
		}
		if frame.FinishReason != "" {
			continue
		}
		collected.WriteString(frame.Token)
		if !req.Stream {
			continue
		}
		chunk := map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"model":   req.Model,
			"choices": []any{map[string]any{"index": frame.Index, "delta": map[string]any{"content": frame.Token}, "finish_reason": nil}},
		}
		raw, _ := json.Marshal(chunk)
		sendChunk(string(raw))
		select {
		case <-r.Context().Done():
			return outcome{ok: false, sentHeaders: true, err: r.Context().Err()}
		default:
		}
	}
}
