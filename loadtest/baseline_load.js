import http from 'k6/http';
import { check, sleep } from 'k6';

// Steady-state load testing to evaluate normal baseline operation
export const options = {
  stages: [
    { duration: '30s', target: 20 }, // Ramp up to 20 users
    { duration: '3m', target: 20 },  // Maintain 20 users
    { duration: '30s', target: 0 },  // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests should be below 500ms
    http_req_failed: ['rate<0.05'],   // Less than 5% failure
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:30080';

export default function () {
  // 1. Hit order creation endpoint (CPU intensive)
  const orderPayload = JSON.stringify({
    items: ['PROD-1', 'PROD-2'],
    customer: 'load-tester',
  });

  const orderParams = {
    headers: { 'Content-Type': 'application/json' },
  };

  const orderRes = http.post(`${BASE_URL}/api/gateway/orders`, orderPayload, orderParams);
  check(orderRes, {
    'order status 200 or 500 (handled)': (r) => r.status === 200 || r.status === 500,
  });

  // 2. Hit inventory lookup endpoint
  const randomProdId = `PROD-${Math.floor(Math.random() * 50) + 1}`;
  const invRes = http.get(`${BASE_URL}/api/gateway/inventory/${randomProdId}`);
  check(invRes, {
    'inventory status 200': (r) => r.status === 200,
  });

  sleep(0.5);
}
