import http from 'k6/http';
import { check, sleep } from 'k6';

// Burst load testing: sudden intense spike to trigger aggressive autoscaling
export const options = {
  stages: [
    { duration: '30s', target: 10 },  // Warm up at 10 VUs
    { duration: '10s', target: 80 },  // Sudden surge to 80 VUs
    { duration: '2m', target: 80 },   // Maintain surge during chaos injection
    { duration: '10s', target: 10 },  // Drop back to 10 VUs
    { duration: '30s', target: 0 },   // Cool down
  ],
  thresholds: {
    http_req_duration: ['p(95)<1500'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:30080';

export default function () {
  const orderPayload = JSON.stringify({
    items: ['PROD-10', 'PROD-20', 'PROD-30'],
    customer: 'burst-tester',
  });

  const orderParams = {
    headers: { 'Content-Type': 'application/json' },
  };

  const orderRes = http.post(`${BASE_URL}/api/gateway/orders`, orderPayload, orderParams);
  check(orderRes, {
    'order responded': (r) => r.status > 0,
  });

  // Also do bulk lookups on inventory
  const bulkPayload = JSON.stringify({ count: 25 });
  const invRes = http.post(`${BASE_URL}/api/inventory/bulk-lookup`, bulkPayload, orderParams);
  check(invRes, {
    'bulk lookup responded': (r) => r.status > 0,
  });

  sleep(0.2);
}
