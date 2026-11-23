import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const base = __ENV.BASE_URL || 'http://localhost:8080';
const item = __ENV.ITEM_ID || 'flash-sale-demo';
const adminToken = __ENV.ADMIN_TOKEN || 'dev-admin-change-me';
const reserveLatency = new Trend('reserve_success_duration', true);
const checkoutLatency = new Trend('checkout_accept_duration', true);
const admitted = new Counter('admitted_users');
const paused = new Counter('paused_users');
const soldOut = new Counter('sold_out_users');
const flowErrors = new Rate('customer_flow_errors');

const profiles = {
  spike: {
    scenarios: {
      flash_spike: {
        executor: 'ramping-arrival-rate',
        startRate: 20,
        timeUnit: '1s',
        preAllocatedVUs: 100,
        maxVUs: 2000,
        stages: [
          { target: Number(__ENV.TARGET_RPS || 1750), duration: '30s' },
          { target: Number(__ENV.TARGET_RPS || 1750), duration: __ENV.HOLD || '5m' },
          { target: 20, duration: '30s' },
        ],
      },
    },
  },
  soak: {
    scenarios: {
      flash_soak: {
        executor: 'constant-arrival-rate',
        rate: Number(__ENV.TARGET_RPS || 500),
        timeUnit: '1s',
        duration: __ENV.DURATION || '30m',
        preAllocatedVUs: 200,
        maxVUs: 1200,
      },
    },
  },
  stress: {
    scenarios: {
      flash_stress: {
        executor: 'ramping-arrival-rate',
        startRate: 50,
        timeUnit: '1s',
        preAllocatedVUs: 200,
        maxVUs: 3000,
        stages: [
          { target: 250, duration: '1m' },
          { target: 500, duration: '1m' },
          { target: 1000, duration: '1m' },
          { target: 1750, duration: '1m' },
          { target: 2500, duration: '1m' },
        ],
      },
    },
  },
};

const profile = __ENV.PROFILE || 'spike';
export const options = {
  ...profiles[profile] || profiles.spike,
  thresholds: {
    'reserve_success_duration': ['p(99)<500'],
    'checkout_accept_duration': ['p(99)<500'],
    'http_req_failed': ['rate<0.05'],
  },
};

export default function () {
  const user = `k6-${__VU}-${__ITER}`;
  const headers = { 'Content-Type': 'application/json' };
  let failed = false;

  const join = http.post(`${base}/queue/${item}/join`, JSON.stringify({ user_id: user }), { headers, tags: { phase: 'join' } });
  if (!check(join, { 'join accepted': (r) => r.status === 200 })) {
    flowErrors.add(1);
    return;
  }

  const deadline = Date.now() + Number(__ENV.QUEUE_WAIT_MS || 30000);
  let token = '';
  while (Date.now() < deadline) {
    const poll = http.get(`${base}/queue/status?item=${encodeURIComponent(item)}&user=${encodeURIComponent(user)}`, { tags: { phase: 'poll' } });
    if (poll.status !== 200) {
      failed = true;
      break;
    }
    const state = poll.json();
    if (state.admission_token) {
      token = state.admission_token;
      admitted.add(1);
      break;
    }
    const gate = state.watermark && state.watermark.gate;
    const inventory = state.watermark && state.watermark.inventory_state;
    if (gate === 'PAUSED') paused.add(1);
    if (inventory === 'SOLD_OUT') {
      soldOut.add(1);
      return;
    }
    sleep(Math.max(0.1, Math.min((state.retry_after_ms || 1000) / 1000, 3)) * (0.8 + Math.random() * 0.4));
  }
  if (failed || !token) {
    flowErrors.add(1);
    return;
  }

  const reserve = http.post(`${base}/items/${item}/reserve`, null, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { phase: 'reserve' },
  });
  if (reserve.status !== 201) {
    flowErrors.add(1);
    return;
  }
  reserveLatency.add(reserve.timings.duration);
  const hold = reserve.json();

  const checkout = http.post(`${base}/holds/${hold.id}/checkout`, null, {
    headers: { 'X-Item-ID': item, 'X-User-ID': user },
    tags: { phase: 'checkout' },
  });
  if (checkout.status !== 202) {
    flowErrors.add(1);
    return;
  }
  checkoutLatency.add(checkout.timings.duration);

  const statusURL = `${base}${checkout.json().status_url}?item=${encodeURIComponent(item)}`;
  for (let i = 0; i < 20; i++) {
    const status = http.get(statusURL, { headers: { 'X-User-ID': user }, tags: { phase: 'status' } });
    if (status.status !== 200) {
      failed = true;
      break;
    }
    const order = status.json();
    if (order.status === 'CONFIRMED' || order.status === 'CANCELLED') break;
    sleep(0.25);
  }
  flowErrors.add(failed ? 1 : 0);
}
