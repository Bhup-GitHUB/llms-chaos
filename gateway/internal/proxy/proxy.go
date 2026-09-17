package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bhupesh/llms-chaos/gateway/internal/breaker"
	"github.com/bhupesh/llms-chaos/gateway/internal/hedge"
	"github.com/bhupesh/llms-chaos/gateway/internal/limiter"
	"github.com/bhupesh/llms-chaos/gateway/internal/middleware"
	"github.com/bhupesh/llms-chaos/gateway/internal/router"
	"github.com/bhupesh/llms-chaos/gateway/internal/trace"
)

type Forwarder struct {
	Pool          Pool
	Breakers      *breaker.Registry
	Metrics       *middleware.Metrics
	DialTimeout   time.Duration
	HeaderTimeout time.Duration
	Hedge         *hedge.Budget
	Limiter       *limiter.Limiter
}

type Pool interface {
	Pick() *router.Worker
	PickForKey(string) *router.Worker
	PickExcept(string) *router.Worker
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

var errBusy = errors.New("limiter busy")
var errNoHedge = errors.New("no hedge worker")

type firstOutcome struct {
	worker       *router.Worker
	conn         net.Conn
	br           *bufio.Reader
	firstPayload string
	elapsed      time.Duration
}

type outcome struct {
	ok          bool
	sentHeaders bool
	err         error
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
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
	traceID, traceparent := trace.FromRequest(r)
	w.Header().Set("traceparent", traceparent)
	sessionKey := r.Header.Get("X-Session-Key")
	workerReq := map[string]any{
		"request_id":  requestID,
		"messages":    req.Messages,
		"model":       req.Model,
		"max_tokens":  req.MaxTokens,
		"temperature": req.Temperature,
		"trace_id":    traceID,
	}
	body, _ := json.Marshal(workerReq)

	cleanup := func(o firstOutcome) {
		if o.conn != nil {
			o.conn.Close()
		}
		if o.worker != nil && f.Limiter != nil {
			f.Limiter.Release(o.worker.ID)
		}
	}

	var lastErr error
	prevID := ""
	for attempt := 0; attempt < 2; attempt++ {
		var primary *router.Worker
		if attempt == 0 {
			primary = f.Pool.PickForKey(sessionKey)
		} else {
			f.Metrics.Retry()
			if prevID != "" {
				primary = f.Pool.PickExcept(prevID)
				if primary == nil {
					primary = f.Pool.PickForKey(sessionKey)
				}
			} else {
				primary = f.Pool.PickForKey(sessionKey)
			}
		}
		if primary == nil {
			http.Error(w, "no workers available", http.StatusServiceUnavailable)
			return
		}
		prevID = primary.ID
		b := f.Breakers.For(primary.ID)
		if !b.Allow() {
			continue
		}
		budget := f.HeaderTimeout
		if f.Hedge != nil {
			budget = f.Hedge.Value()
			if budget <= 0 {
				budget = f.HeaderTimeout
			}
			if f.HeaderTimeout > 0 && budget > f.HeaderTimeout {
				budget = f.HeaderTimeout
			}
		}
		primaryFn := func(ctx context.Context) (firstOutcome, error) {
			if f.Limiter != nil && !f.Limiter.TryAcquire(primary.ID) {
				return firstOutcome{}, errBusy
			}
			conn, br, payload, elapsed, err := f.dialFirstToken(ctx, primary, body, traceparent)
			if err != nil {
				if f.Limiter != nil {
					f.Limiter.Release(primary.ID)
					if isTimeout(err) {
						f.Limiter.OnTimeout(primary.ID)
					}
				}
				return firstOutcome{}, err
			}
			return firstOutcome{worker: primary, conn: conn, br: br, firstPayload: payload, elapsed: elapsed}, nil
		}
		secondaryFn := func(ctx context.Context) (firstOutcome, error) {
			alt := f.Pool.PickExcept(primary.ID)
			if alt == nil {
				return firstOutcome{}, errNoHedge
			}
			ba := f.Breakers.For(alt.ID)
			if !ba.Allow() {
				return firstOutcome{}, errNoHedge
			}
			if f.Limiter != nil && !f.Limiter.TryAcquire(alt.ID) {
				return firstOutcome{}, errBusy
			}
			conn, br, payload, elapsed, err := f.dialFirstToken(ctx, alt, body, traceparent)
			if err != nil {
				if f.Limiter != nil {
					f.Limiter.Release(alt.ID)
					if isTimeout(err) {
						f.Limiter.OnTimeout(alt.ID)
					}
				}
				return firstOutcome{}, err
			}
			return firstOutcome{worker: alt, conn: conn, br: br, firstPayload: payload, elapsed: elapsed}, nil
		}
		var winner firstOutcome
		var werr error
		var hedged bool
		if f.Hedge != nil {
			winner, werr, hedged = hedge.RaceWithCleanup(r.Context(), budget, primaryFn, secondaryFn, cleanup)
		} else {
			winner, werr = primaryFn(r.Context())
			hedged = false
		}
		if hedged {
			f.Metrics.Retry()
		}
		if werr != nil {
			if errors.Is(werr, errBusy) {
				f.Metrics.Reject()
				http.Error(w, "gateway overloaded", http.StatusTooManyRequests)
				return
			}
			b.Record(false)
			if r.Context().Err() != nil {
				return
			}
			lastErr = werr
			continue
		}
		res := f.streamFromFirstToken(winner.worker, winner.conn, winner.br, winner.firstPayload, winner.elapsed, requestID, req, w, r)
		if f.Limiter != nil {
			f.Limiter.Release(winner.worker.ID)
			if res.ok {
				f.Limiter.OnSuccess(winner.worker.ID)
			} else if res.err != nil && isTimeout(res.err) {
				f.Limiter.OnTimeout(winner.worker.ID)
			}
		}
		wb := f.Breakers.For(winner.worker.ID)
		wb.Record(res.ok)
		if f.Hedge != nil && (res.ok || res.sentHeaders) {
			f.Hedge.Observe(winner.elapsed)
		}
		if winner.worker != nil {
			winner.worker.Observe(uint64(winner.elapsed.Milliseconds()))
		}
		if res.ok || res.sentHeaders {
			return
		}
		lastErr = res.err
	}
	if lastErr != nil {
		http.Error(w, "upstream failed", http.StatusBadGateway)
		return
	}
	http.Error(w, "upstream failed", http.StatusBadGateway)
}

func (f *Forwarder) dialFirstToken(ctx context.Context, wrk *router.Worker, body []byte, traceparent string) (net.Conn, *bufio.Reader, string, time.Duration, error) {
	start := time.Now()
	addr := fmt.Sprintf("%s:%d", wrk.Host, wrk.Port)
	dialer := &net.Dialer{Timeout: f.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, "", 0, err
	}
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	deadline := time.Now().Add(f.HeaderTimeout + 120*time.Second)
	_ = conn.SetDeadline(deadline)
	fmt.Fprintf(conn, "POST /generate HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nTraceparent: %s\r\nConnection: close\r\n\r\n", addr, len(body), traceparent)
	if _, err := conn.Write(body); err != nil {
		conn.Close()
		return nil, nil, "", 0, err
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, nil, "", 0, err
	}
	if !strings.Contains(status, "200") {
		conn.Close()
		return nil, nil, "", 0, fmt.Errorf("worker status: %s", strings.TrimSpace(status))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, nil, "", 0, err
		}
		if line == "\r\n" {
			break
		}
	}
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			return nil, nil, "", 0, ctx.Err()
		default:
		}
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, nil, "", 0, err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
		return conn, br, payload, time.Since(start), nil
	}
}

func (f *Forwarder) streamFromFirstToken(wrk *router.Worker, conn net.Conn, br *bufio.Reader, firstPayload string, elapsed time.Duration, requestID string, req ChatRequest, w http.ResponseWriter, r *http.Request) outcome {
	defer conn.Close()
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
	first := true
	observeFirst := func() {
		if first {
			f.Metrics.ObserveTTFT(elapsed.Seconds())
			first = false
		}
	}
	if firstPayload == "[DONE]" {
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
	var firstFrame workerFrame
	if err := json.Unmarshal([]byte(firstPayload), &firstFrame); err == nil {
		observeFirst()
		if firstFrame.FinishReason == "" {
			collected.WriteString(firstFrame.Token)
			if req.Stream {
				chunk := map[string]any{
					"id":      requestID,
					"object":  "chat.completion.chunk",
					"model":   req.Model,
					"choices": []any{map[string]any{"index": firstFrame.Index, "delta": map[string]any{"content": firstFrame.Token}, "finish_reason": nil}},
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
		observeFirst()
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
