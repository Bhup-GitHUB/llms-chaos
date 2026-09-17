import argparse
import json
import sys
import time
import urllib.request
import urllib.error


def percentile(sorted_values, q):
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = q / 100.0 * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    frac = rank - low
    return float(sorted_values[low] + (sorted_values[high] - sorted_values[low]) * frac)


def guess_worker(headers, body_text):
    for key in ("X-Worker-Id", "X-Served-By", "X-Worker", "X-Upstream-Worker"):
        value = headers.get(key) if hasattr(headers, "get") else None
        if value:
            return str(value)
    if not body_text:
        return "unknown"
    try:
        payload = json.loads(body_text)
    except Exception:
        return "unknown"
    for key in ("worker_id", "worker", "served_by", "upstream"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if value:
            return str(value)
    try:
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                for key in ("worker_id", "worker", "served_by"):
                    if first.get(key):
                        return str(first.get(key))
    except Exception:
        pass
    return "unknown"


def single_request(base_url, timeout, max_tokens, stream):
    url = base_url.rstrip("/") + "/v1/chat/completions"
    body = {
        "model": "sim",
        "messages": [{"role": "user", "content": "measure ttft under slo evaluation"}],
        "max_tokens": max_tokens,
        "stream": stream,
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "text/event-stream" if stream else "application/json"},
        method="POST",
    )
    start = time.perf_counter()
    ttft = None
    worker = "unknown"
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
            headers = resp.headers
            if stream:
                chunks = []
                while True:
                    line = resp.readline()
                    if not line:
                        break
                    if ttft is None and line.strip() and line.strip() != b":":
                        ttft = time.perf_counter() - start
                    chunks.append(line)
                    if b"[DONE]" in line:
                        break
                body_text = b"".join(chunks).decode("utf-8", errors="replace")
            else:
                body_text = resp.read().decode("utf-8", errors="replace")
                ttft = time.perf_counter() - start
            if status != 200:
                return False, timeout, "unknown"
            worker = guess_worker(headers, body_text if not stream else "")
            if ttft is None:
                ttft = time.perf_counter() - start
            return True, float(ttft), worker
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - start
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        worker = guess_worker(e.headers, raw)
        return False, float(elapsed), worker
    except Exception:
        elapsed = time.perf_counter() - start
        return False, float(elapsed), "unknown"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--stream", action="store_true")
    args = parser.parse_args()
    latencies = []
    per_worker = {}
    errors = 0
    total = max(1, args.requests)
    for _ in range(total):
        ok, ttft, worker = single_request(args.base_url, args.timeout, args.max_tokens, args.stream)
        per_worker[worker] = per_worker.get(worker, 0) + 1
        if ok:
            latencies.append(float(ttft))
        else:
            errors += 1
    ordered = sorted(latencies)
    p50 = percentile(ordered, 50)
    p99 = percentile(ordered, 99)
    error_rate = errors / float(total)
    counts = list(per_worker.values())
    spread = (max(counts) / min(counts)) if counts and min(counts) > 0 else float(total)
    result = {
        "base_url": args.base_url,
        "requests": total,
        "errors": errors,
        "error_rate": error_rate,
        "ttft_p50_s": p50,
        "ttft_p99_s": p99,
        "ttft_samples": len(latencies),
        "per_worker_counts": per_worker,
        "per_worker_spread_max_min": spread,
    }
    sys.stdout.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
