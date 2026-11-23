#!/usr/bin/env python3
"""Render the final HLD as a readable SVG/PNG from a deterministic layout."""
from pathlib import Path
import html
import cairosvg

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "diagrams"
W, H = 3000, 2370
parts = []


def esc(value: str) -> str:
    return html.escape(value, quote=True)


def rect(x, y, w, h, fill="#ffffff", stroke="#64748b", radius=18, sw=2, dash=None):
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{dash_attr}/>'
    )


def text(x, y, value, size=18, fill="#17212f", weight="400", anchor="start"):
    parts.append(
        f'<text x="{x}" y="{y}" font-family="Inter,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(value)}</text>'
    )


def multiline(x, y, lines, size=15, fill="#334155", line=22, weight="400"):
    for i, value in enumerate(lines):
        text(x, y + i * line, value, size=size, fill=fill, weight=weight)


def heading(x, y, value, color="#17212f", size=23):
    text(x, y, value, size=size, fill=color, weight="700")


def panel(x, y, w, h, title, fill, stroke, subtitle=None):
    rect(x, y, w, h, fill=fill, stroke=stroke, radius=22, sw=2)
    heading(x + 24, y + 38, title, color=stroke, size=21)
    if subtitle:
        text(x + 24, y + 65, subtitle, size=14, fill="#475569")


def card(x, y, w, h, title, lines, fill="#ffffff", stroke="#94a3b8", number=None, title_color="#17212f"):
    rect(x, y, w, h, fill=fill, stroke=stroke, radius=16, sw=2)
    tx = x + 22
    if number is not None:
        parts.append(f'<circle cx="{x + 31}" cy="{y + 31}" r="17" fill="#1d4ed8"/>')
        text(x + 31, y + 37, str(number), size=17, fill="#ffffff", weight="700", anchor="middle")
        tx = x + 60
    text(tx, y + 38, title, size=19, fill=title_color, weight="700")
    multiline(x + 22, y + 73, lines, size=15, fill="#334155", line=22)


def arrow(points, label=None, dashed=False, color="#334155", label_x=None, label_y=None, width=2.5):
    pts = " ".join(f"{x},{y}" for x, y in points)
    dash = ' stroke-dasharray="9 7"' if dashed else ""
    parts.append(
        f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}" '
        f'stroke-linecap="round" stroke-linejoin="round" marker-end="url(#arrow)"{dash}/>'
    )
    if label:
        lx = label_x if label_x is not None else points[len(points)//2][0]
        ly = label_y if label_y is not None else points[len(points)//2][1] - 10
        pad = 9
        tw = max(85, len(label) * 8.1 + pad * 2)
        rect(lx - tw/2, ly - 19, tw, 27, fill="#ffffff", stroke="#cbd5e1", radius=8, sw=1)
        text(lx, ly, label, size=13, fill=color, weight="600", anchor="middle")


parts.append(f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
<defs>
  <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="4" orient="auto" markerUnits="strokeWidth">
    <path d="M0,0 L8,4 L0,8 z" fill="#334155"/>
  </marker>
</defs>
<rect width="100%" height="100%" fill="#ffffff"/>
''')

# Header.
heading(70, 72, "SPIKE-RESILIENT FLASH SALE & TICKETING PLATFORM", size=30)
text(70, 108, "Final high-level design | Read numbered cards: Row 1 left-to-right →, Row 2 right-to-left ←, Row 3 left-to-right →. Lower panels show deployment boundaries and recovery paths.", size=16, fill="#475569")

# Main flow section.
panel(50, 140, 2900, 840, "PRIMARY REQUEST + ORDER FLOW (1–15)", fill="#f8fafc", stroke="#64748b", subtitle="Customer-facing path is fast and bounded; order creation, authorization and capture are asynchronous after checkout acceptance. Row directions are explicitly serpentine.")

card_x = [80, 665, 1250, 1835, 2420]
card_w, card_h = 500, 190
rows = [220, 470, 720]

row1 = [
    (1, "BROWSE / AVAILABILITY", ["~50:1 read/write load", "CDN absorbs catalog + availability reads", "Availability cache: 1s + stale-while-revalidate", "Cached counts never allocate inventory"], "#eff6ff", "#60a5fa"),
    (2, "JOIN / POLL WAITING ROOM", ["~500K edge polls/s (10M / 20s poll assumption)", "WAF: bot management + IP/token/account limits", "Queue polls served from cached watermark; jittered", "PAUSED: preserve place; issue no admission token", "Terminal SOLD_OUT returns a final queue answer"], "#fff7ed", "#fb923c"),
    (3, "ADMISSION CONTROL", ["Publishes 1s watermark + admission gate from HA CTRL", "OPEN / TEMP_UNAVAILABLE / SOLD_OUT inventory states", "PAUSED admission gate is not terminal SOLD_OUT", "Pause on unknown stock, PSP breaker / cap, lost CTRL", "or stale / missing remote standby ACK"], "#ecfdf5", "#34d399"),
    (4, "SIGNED ADMISSION TOKEN", ["Waiting room returns short-lived signed token", "Offline verification at gateway", "Status + PSP webhook use separate auth"], "#eff6ff", "#60a5fa"),
    (5, "API GATEWAY", ["Admission check for reserve / checkout", "Priority rate limits and load shedding", "~1,750 admitted/s assumed safe cap", "Reservation-path p99 SLO <500 ms"], "#f8fafc", "#94a3b8"),
]
for x, item in zip(card_x, row1):
    n, title, lines, fill, stroke = item
    card(x, rows[0], card_w, card_h, title, lines, fill=fill, stroke=stroke, number=n)

# Row 1 arrows.
for i in range(4):
    x1 = card_x[i] + card_w + 7
    x2 = card_x[i+1] - 8
    arrow([(x1, rows[0] + 95), (x2, rows[0] + 95)])

row2_x = [2420, 1835, 1250, 665, 80]  # snake back from right to left
row2 = [
    (6, "RESERVATION ROUTER", ["Specific seat: hash(unit_id) % S", "Any unit: walk from hash(user_id) % S", "HSETNX per-hold key: hold-claim:{base64url(id)}", "Bound shard drains → SOLD_OUT; never remap"], "#eff6ff", "#60a5fa"),
    (7, "REDIS ATOMIC HOLD", ["Lua: unit → HELD with TTL + deadline", "Checkout: HELD → PAYING; return 202 + URL", "Same-slot outbox event", "Reaper never frees PAYING on timer evidence"], "#eff6ff", "#60a5fa"),
    (8, "OUTBOX RELAY", ["One active lease owner per Redis stream", "Publish then ACK only after broker ACK", "Pending entries replayed after relay restart"], "#f1f5f9", "#94a3b8"),
    (9, "KAFKA API EVENT LOG", ["12 partitions; partition key = order_id", "Per-order ordering; at-least-once delivery", "Retry → requeue → DLQ; offset after durable outcome"], "#f1f5f9", "#94a3b8"),
    (10, "ORDER WORKERS", ["Bulkhead + per-PSP-call token bucket", "Create order/payment intent before PSP", "Authorize + confirm; capture is separate"], "#f0fdf4", "#4ade80"),
]
for x, item in zip(row2_x, row2):
    n, title, lines, fill, stroke = item
    card(x, rows[1], card_w, card_h, title, lines, fill=fill, stroke=stroke, number=n)
for i in range(4):
    x1 = row2_x[i] - 8
    x2 = row2_x[i+1] + card_w + 7
    arrow([(x1, rows[1] + 95), (x2, rows[1] + 95)])

row3 = [
    (11, "POSTGRES: ORDER INTENT", ["Local WAL flush only: PENDING_PAYMENT + INTENT", "Not yet cross-region durable or CONFIRMED", "UNIQUE live (item_id, unit_id); ≤50K live owners/item", "Unique conflict → CANCELLED; short-circuit, NEVER PSP"], "#eff6ff", "#60a5fa"),
    (12, "PSP AUTHORIZE", ["Idempotency-Key = order_id", "Metadata: item / unit / user / home region / epoch", "≤1,400 auth calls/s; timeout = UNKNOWN", "Query/retry same key; void before cancel/release"], "#fff7ed", "#f59e0b"),
    (13, "CROSS-REGION SYNC ACK", ["CAS AUTHORIZED + CONFIRMED only after PSP auth", "Remote standby WAL-flush ACK; primary + standby (2-of-2)", "No ACK → pause confirms and new admissions", "Expose/project CONFIRMED only after durable commit", "Pre-ACK region loss: REC rebuilds from PSP metadata; void orphan auth"], "#eff6ff", "#60a5fa"),
    (14, "FINALIZE INVENTORY", ["After remote ACK: mark Redis unit sold; never re-offer", "Unique loser: short-circuit from 11; NEVER calls PSP", "Forfeit loser unit pending reconciliation", "Definitive decline/void: CANCELLED + safe release"], "#ecfdf5", "#34d399"),
    (15, "ORDER STATUS READ", ["KV fast path; monotonic status version", "Miss/lag → authoritative PostgreSQL fallback", "Pre-order-row window → Redis PAYING/PROCESSING"], "#f8fafc", "#94a3b8"),
]
for x, item in zip(card_x, row3):
    n, title, lines, fill, stroke = item
    card(x, rows[2], card_w, card_h, title, lines, fill=fill, stroke=stroke, number=n)
for i in range(4):
    x1 = card_x[i] + card_w + 7
    x2 = card_x[i+1] - 8
    arrow([(x1, rows[2] + 95), (x2, rows[2] + 95)])

# Unique-index loser branches from step 11 directly to step 14; it never crosses PSP step 12.
arrow([(card_x[0] + card_w, rows[2] - 14), (card_x[3] + card_w / 2, rows[2] - 14)],
      label="unique conflict: cancel + bypass PSP", dashed=True, color="#b45309",
      label_x=1335, label_y=rows[2] - 20, width=2)

# Connect row transitions. Step 5 -> 6; step 10 -> 11.
arrow([(2670, rows[0] + card_h + 4), (2670, rows[1] - 9)], label="reserve / checkout request", label_x=2670, label_y=rows[1] - 17)
arrow([(330, rows[1] + card_h + 4), (330, rows[2] - 9)], label="durable order processing", label_x=480, label_y=rows[2] - 17)

# Deployment and failure boundaries.
text(70, 1030, "DEPLOYMENT / FAILURE BOUNDARIES", size=23, fill="#17212f", weight="700")

# Home region AZ panel.
panel(50, 1060, 1450, 500, "HOME REGION — SINGLE WRITER FOR THIS ITEM", fill="#f8fafc", stroke="#64748b", subtitle="Two home-region AZs; stateless app replicas and leased workers in both; item ownership is fenced.")
panel(80, 1145, 680, 335, "AZ a — APP + WORKERS", fill="#ffffff", stroke="#94a3b8")
card(105, 1220, 295, 175, "APP TIER", ["GW: offline token verification", "RES: reserve / checkout / status", "PWH: signed PSP webhook ingress"], fill="#eff6ff", stroke="#60a5fa")
card(425, 1220, 310, 175, "WORKER TIER", ["ADM / RLY / REAP / WRK", "CAP / REC; leased ownership", "No role pinned to AZ a"], fill="#ecfdf5", stroke="#34d399")
panel(790, 1145, 680, 335, "AZ b — REPLICAS", fill="#ffffff", stroke="#94a3b8")
card(815, 1220, 295, 175, "APP REPLICAS", ["Same gateway / reservation paths", "N+1 capacity; no AZ affinity", "Load-balanced from WAF"], fill="#eff6ff", stroke="#60a5fa")
card(1135, 1220, 310, 175, "WORKER REPLICAS", ["Same worker roles as AZ a", "Lease / partition takeover", "Higher fencing epoch required"], fill="#ecfdf5", stroke="#34d399")
text(90, 1525, "On AZ loss: surviving replicas take over only after lease + fencing checks; checkout / payment work remains idempotent.", size=15, fill="#475569")

# Data tier panel.
panel(1530, 1060, 1420, 500, "DATA TIER — MULTI-AZ FAILURE BOUNDARY", fill="#eff6ff", stroke="#2563eb", subtitle="Inventory, admission control, durable ownership, event log and projections have separate responsibilities.")
# Three columns x three compact rows; CTRL has a dedicated HA keyspace/partition.
data_cards = [
    (1560, 1145, "REDIS INVENTORY", ["unit tokens, holds, deadlines", "same-slot Lua + per-shard outbox", "isolated inventory shard keys"], "#f8fbff", "#60a5fa"),
    (2010, 1145, "HA ADMISSION CONTROL", ["dedicated replicated CTRL keyspace", "stock + 3-state watermark; admission gate", "PSP capacity + remote standby ACK health"], "#f0fdfa", "#0f766e"),
    (2460, 1145, "KAFKA API LOG", ["12 partitions by order_id", "per-order ordering; at-least-once", "offset after durable handling / DLQ"], "#f8fbff", "#60a5fa"),
    (1560, 1285, "POSTGRES + POOLER", ["orders, payments, durable idempotency", "CAS + UNIQUE live (item_id, unit_id)", "≤50K simultaneous live owners per hot item"], "#f8fbff", "#60a5fa"),
    (2010, 1285, "KEY-VALUE READ MODEL", ["versioned order/payment status", "catalog documents", "projection only; not payment authority"], "#f8fbff", "#60a5fa"),
    (2460, 1285, "DLQ", ["after bounded retries / requeue", "REC triage; audited replay", "no blind automatic replay"], "#f8fbff", "#60a5fa"),
    (1560, 1425, "ANALYTICS STORE", ["columnar lifecycle aggregates", "fed asynchronously from event log", "isolated from checkout"], "#f8fbff", "#60a5fa"),
]
for x, y, title, lines, fill, stroke in data_cards:
    card(x, y, 420, 130, title, lines, fill=fill, stroke=stroke)

# Supporting flows / failure boundaries.
text(70, 1625, "SUPPORTING FLOWS, EXTERNAL SYSTEMS AND RECOVERY", size=23, fill="#17212f", weight="700")

support_y, support_h, support_w, gap = 1655, 640, 700, 25
support_x = [50, 50 + support_w + gap, 50 + 2 * (support_w + gap), 50 + 3 * (support_w + gap)]

panel(support_x[0], support_y, support_w, support_h, "EXTERNAL PSP — FAILURE BOUNDARY", fill="#fff7ed", stroke="#b7791f")
card(support_x[0] + 25, support_y + 95, 310, 185, "PSP", ["rate limited; timeout / lost response", "authorize / capture / void / query", "idempotency contract is required"], fill="#ffffff", stroke="#f59e0b")
card(support_x[0] + 365, support_y + 95, 310, 185, "WEBHOOK INGRESS", ["PSP → WAF → PWH → event log", "signature + timestamp verification", "dedupe event ID; bypass waiting room"], fill="#ffffff", stroke="#f59e0b")
multiline(support_x[0] + 25, support_y + 335, ["Polling by the same idempotency key is the fallback for", "missing or late callbacks. PSP timeout is UNKNOWN, never decline."], size=15, line=24)

panel(support_x[1], support_y, support_w, support_h, "CAPTURE + HOLD LIFECYCLE", fill="#f0fdf4", stroke="#16a34a")
card(support_x[1] + 25, support_y + 95, 310, 185, "CAPTURE SWEEPER", ["DB: CONFIRMED + AUTHORIZED", "deferred idempotent capture", "per-call PSP token bucket"], fill="#ffffff", stroke="#4ade80")
card(support_x[1] + 365, support_y + 95, 310, 185, "HOLD REAPER", ["expired HELD → release to sale", "PAYING → HoldPayOverdue", "Redis outbox → RLY → Kafka → WRK/REC", "never release PAYING on timer"], fill="#ffffff", stroke="#4ade80")
multiline(support_x[1] + 25, support_y + 335, ["WRK → dedicated HA CTRL breaker/capacity → ADM → watermark;", "DB publishes standby-ACK health; stale/missing health pauses new admits.", "MEM publishes stock summary into CTRL; inventory shards stay separate.", "KV: PENDING after local DB commit; CONFIRMED after remote ACK;", "CANCELLED after definitive failure; CAPTURED after CAP DB CAS."], size=14, line=23)

panel(support_x[2], support_y, support_w, support_h, "DLQ + RECONCILIATION", fill="#f8fafc", stroke="#64748b")
card(support_x[2] + 25, support_y + 95, 310, 185, "DLQ ROUTE", ["worker bounded retry → requeue → DLQ", "REC monitors and triages", "replay only after remediation"], fill="#ffffff", stroke="#94a3b8")
card(support_x[2] + 365, support_y + 95, 310, 185, "RECONCILER", ["compare Redis / DB / PSP ledger", "detect orphans, duplicates, stuck holds", "fix only stale, terminal-safe records"], fill="#ffffff", stroke="#94a3b8")
multiline(support_x[2] + 25, support_y + 335, ["HoldPayOverdue and failed payment events are resolved against", "durable order/payment state and PSP truth before release."], size=15, line=24)

panel(support_x[3], support_y, support_w, support_h, "REMOTE REGIONS + STANDBY + TELEMETRY", fill="#f0fdfa", stroke="#0f766e")
card(support_x[3] + 25, support_y + 95, 310, 185, "OTHER REGIONS", ["edge / waiting room / reads local", "all writes forward to home gateway", "item route + fencing epoch"], fill="#ffffff", stroke="#2dd4bf")
card(support_x[3] + 365, support_y + 95, 310, 185, "STANDBY REGION", ["PostgreSQL synchronous standby", "remote WAL-flush ACK required", "RPO 0 for acknowledged CONFIRMED", "Kafka log replica is asynchronous"], fill="#ffffff", stroke="#2dd4bf")
multiline(support_x[3] + 25, support_y + 335, ["Primary + one remote standby are both required (2-of-2), not majority quorum.", "WAN/standby ACK loss pauses confirmations and new admission tokens.", "PAUSED preserves queue place; browse remains available.", "PAYING holds stay locked; REAP emits HoldPayOverdue via Redis outbox,", "then RLY -> Kafka -> WRK/REC; PAYING is never timer-released.", "Step-11 INTENT is local WAL only, not remote durable.", "On region loss after PSP auth but before step-13 ACK:", "fence old writer; acquire higher epoch; query PSP by idempotency key.", "Recover metadata; verify standby commit; reconstruct missing recovery record.", "Void orphan auth; reconcile DB / PSP ledger / inventory before reopening.", "PSP metadata must be queryable by key; OTel / Prometheus never gates sale."], size=14, line=23)

# Footer legend.
text(70, 2340, "Strict invariants: no duplicate live unit owner • no duplicate PSP effect per idempotency key • CONFIRMED only after remote WAL flush ACK • failures prefer temporary undersell.", size=15, fill="#475569", weight="600")
parts.append("</svg>")

svg = "\n".join(parts)
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "hld.svg").write_text(svg, encoding="utf-8")
cairosvg.svg2png(bytestring=svg.encode("utf-8"), write_to=str(OUT / "hld.png"), output_width=W, output_height=H)
cairosvg.svg2pdf(bytestring=svg.encode("utf-8"), write_to=str(OUT / "hld.pdf"))
print(f"Wrote {OUT / 'hld.svg'}, {OUT / 'hld.png'} and {OUT / 'hld.pdf'} ({W}x{H})")
