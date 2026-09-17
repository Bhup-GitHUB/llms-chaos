up:
	docker compose up -d toxiproxy prometheus grafana loki

down:
	docker compose down

gateway:
	go run ./gateway/cmd/gateway

worker:
	WORKER_ID=worker-1 WORKER_PORT=9001 uvicorn app.main:app --host 0.0.0.0 --port 9001

chaos-kill:
	go run ./chaosd/cmd/chaos kill worker-2

slos:
	curl -s localhost:8000/metrics | grep -E "ttft|request_duration|worker_healthy"
