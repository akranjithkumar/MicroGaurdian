import http from 'k6/http';
import { check, sleep } from 'k6';

// Multi-step progressive ramp load test
export const options = {
  stages: [
    { duration: '1m', target: 15 },
    { duration: '1m', target: 30 },
    { duration: '1m', target: 60 },
    { duration: '1m', target: 30 },
    { duration: '1m', target: 0 },
  ],
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:30080';

export default function () {
  const orderPayload = JSON.stringify({
    items: ['PROD-5', 'PROD-15'],
  });

  const orderRes = http.post(`${BASE_URL}/api/gateway/orders`, orderPayload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(orderRes, {
    'request handled': (r) => r.status === 200 || r.status === 500,
  });

  sleep(0.3);
}
