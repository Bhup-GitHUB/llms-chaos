import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "1m", target: 10 },
    { duration: "2m", target: 50 },
    { duration: "1m", target: 100 },
    { duration: "1m", target: 0 },
  ],
  thresholds: {
    http_req_failed: ["rate<0.02"],
  },
};

export default function () {
  const payload = JSON.stringify({
    model: "sim",
    messages: [{ role: "user", content: "explain retry budgets under chaos" }],
    max_tokens: 32,
    stream: false,
  });
  const res = http.post("http://localhost:8000/v1/chat/completions", payload, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
  sleep(0.2);
}
