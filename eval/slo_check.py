import argparse
import json
import sys


def load_measure(path):
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("measure", help="Path to measure.py JSON output, or - for stdin")
    parser.add_argument("--max-p99-ttft", type=float, default=2.0)
    parser.add_argument("--max-error-rate", type=float, default=0.02)
    parser.add_argument("--min-workers", type=int, default=2)
    parser.add_argument("--max-spread", type=float, default=4.0)
    args = parser.parse_args()
    data = load_measure(args.measure)
    breaches = []
    p99 = float(data.get("ttft_p99_s", float("inf")))
    error_rate = float(data.get("error_rate", 1.0))
    per_worker = data.get("per_worker_counts", {}) or {}
    spread = float(data.get("per_worker_spread_max_min", 1.0))
    requests = int(data.get("requests", 0))
    if p99 > args.max_p99_ttft:
        breaches.append({"budget": "ttft_p99_s", "value": p99, "threshold": args.max_p99_ttft})
    if error_rate > args.max_error_rate:
        breaches.append({"budget": "error_rate", "value": error_rate, "threshold": args.max_error_rate})
    distinct = len([key for key, count in per_worker.items() if key != "unknown" and count > 0])
    if requests >= 10 and "unknown" not in per_worker and distinct < args.min_workers:
        breaches.append({"budget": "per_worker_distinct", "value": distinct, "threshold": args.min_workers})
    if requests >= 30 and spread > args.max_spread:
        breaches.append({"budget": "per_worker_spread_max_min", "value": spread, "threshold": args.max_spread})
    result = {
        "ok": len(breaches) == 0,
        "breaches": breaches,
        "observed": {
            "ttft_p99_s": p99,
            "ttft_p50_s": float(data.get("ttft_p50_s", 0.0)),
            "error_rate": error_rate,
            "per_worker_counts": per_worker,
            "per_worker_spread_max_min": spread,
        },
    }
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    if breaches:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
