import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 20,
  duration: "3m",
  thresholds: {
    http_req_failed: ["rate<0.05"],
  },
};

export default function () {
  const payload = JSON.stringify({
    model: "sim",
    messages: [{ role: "user", content: "stream tokens while a worker is dead" }],
    max_tokens: 48,
    stream: true,
  });
  const res = http.post("http://localhost:8000/v1/chat/completions", payload, {
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
}
