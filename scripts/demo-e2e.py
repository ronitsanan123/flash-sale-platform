#!/usr/bin/env python3
"""Exercise the actual HTTP API from queue admission through PSP capture.

By default this starts the in-memory Go demo server. With DEMO_EXTERNAL=1 it
runs against BASE_URL, allowing the same flow to smoke-test Docker Compose.
"""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ITEM = os.environ.get("ITEM_ID", "flash-sale-e2e")
USER = os.environ.get("USER_ID", "assignment-demo-user")
EXPECTED_UNITS = int(os.environ.get("EXPECTED_UNITS", "5"))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(base, path, method="GET", body=None, headers=None, expected=(200,)):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method, headers=headers or {})
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=4) as response:
            raw = response.read()
            if response.status not in expected:
                raise RuntimeError(f"{method} {path}: unexpected HTTP {response.status}: {raw!r}")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        if exc.code in expected:
            return json.loads(raw) if raw else {}
        raise RuntimeError(f"{method} {path}: HTTP {exc.code}: {raw}") from exc


def main():
    external = os.environ.get("DEMO_EXTERNAL") == "1"
    proc = None
    if external:
        base = os.environ.get("BASE_URL", "http://127.0.0.1:8080").rstrip("/")
    else:
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        env.update({
            "MODE": "demo",
            "HTTP_ADDR": f"127.0.0.1:{port}",
            "ITEM_ID": ITEM,
            "SEED_UNITS": str(EXPECTED_UNITS),
            "HOME_REGION": "assignment-demo-region",
            "REGION_WRITER_ID": "assignment-demo-primary",
            "ADMIN_TOKEN": "local-e2e-admin-token",
        })
        proc = subprocess.Popen(
            ["go", "run", "./cmd/flashsale"],
            cwd=os.path.join(ROOT, "prototype"),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    try:
        deadline = time.time() + (60 if external else 90)
        while time.time() < deadline:
            if proc is not None and proc.poll() is not None:
                raise RuntimeError("flashsale demo exited during startup:\n" + (proc.stdout.read() or ""))
            try:
                health = request(base, "/healthz")
                if health:
                    break
            except Exception:
                time.sleep(0.25)
        else:
            raise RuntimeError("timed out waiting for API health")

        before = request(base, f"/items/{ITEM}/availability")
        assert before.get("available") == EXPECTED_UNITS, f"expected {EXPECTED_UNITS} units before sale, got {before}"

        joined = request(base, f"/queue/{ITEM}/join", "POST", {"user_id": USER})
        assert joined.get("position") == 1, f"unexpected queue entry: {joined}"

        status_path = "/queue/status?" + urllib.parse.urlencode({"item": ITEM, "user": USER})
        admission = None
        deadline = time.time() + 20
        while time.time() < deadline:
            state = request(base, status_path)
            if state.get("admitted") and state.get("admission_token"):
                admission = state["admission_token"]
                break
            time.sleep(0.2)
        assert admission, f"user was not admitted: {state}"

        hold = request(
            base, f"/items/{ITEM}/reserve", "POST",
            headers={"Authorization": "Bearer " + admission},
            expected=(201,),
        )
        hold_id = hold.get("id")
        assert hold_id, f"reservation did not return a hold ID: {hold}"

        checkout = request(
            base, f"/holds/{hold_id}/checkout", "POST",
            headers={"X-Item-ID": ITEM, "X-User-ID": USER},
            expected=(202,),
        )
        assert checkout.get("order_id") == hold_id, f"checkout mismatch: {checkout}"

        order_path = "/orders/" + urllib.parse.quote(hold_id) + "?" + urllib.parse.urlencode({"item": ITEM})
        order = None
        deadline = time.time() + 45
        while time.time() < deadline:
            order = request(base, order_path, headers={"X-User-ID": USER})
            if order.get("status") == "CONFIRMED" and order.get("payment_status") == "CAPTURED":
                break
            time.sleep(0.25)
        assert order and order.get("status") == "CONFIRMED", f"order not confirmed: {order}"
        assert order.get("payment_status") == "CAPTURED", f"payment not captured: {order}"

        after = request(base, f"/items/{ITEM}/availability")
        deadline = time.time() + 8
        while time.time() < deadline and (after.get("confirmed") != 1 or after.get("available") != EXPECTED_UNITS - 1):
            time.sleep(0.25)  # availability is intentionally cached for one second
            after = request(base, f"/items/{ITEM}/availability")
        assert after.get("confirmed") == 1 and after.get("available") == EXPECTED_UNITS - 1, f"inventory invariant failed: {after}"

        print("http_e2e=PASS")
        print(f"queue_position={joined['position']} admitted=true hold_id={hold_id}")
        print(f"order_status={order['status']} payment_status={order['payment_status']}")
        print(f"inventory_available={after['available']} inventory_confirmed={after['confirmed']} total={after.get('total')}")
        print("flow=health -> availability -> queue join/poll -> signed admission -> reserve -> checkout(202) -> async worker -> PSP authorize -> confirm -> capture -> status")
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"http_e2e=FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
