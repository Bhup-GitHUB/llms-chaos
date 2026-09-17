import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 5,
  duration: "30m",
  thresholds: {
    http_req_failed: ["rate<0.02"],
    http_req_duration: ["p(99)<2000"],
  },
};

export default function () {
  const payload = JSON.stringify({
    model: "sim",
    messages: [{ role: "user", content: "soak gateway at low rps and watch ttft" }],
    max_tokens: 32,
    stream: false,
  });
  const res = http.post("http://localhost:8000/v1/chat/completions", payload, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
  sleep(1.0);
}
