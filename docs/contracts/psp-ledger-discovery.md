# PSP Authorization Ledger Discovery Contract

**Status:** Implemented in the local mock PSP and documented as a required production-provider contract. Unit tests cover pagination, stable snapshots, metadata, delayed indexing and complete-through behaviour; Docker-backed end-to-end recovery has not been run in this environment.

## Why this contract exists

Step 11 writes `PENDING_PAYMENT + INTENT` with local WAL durability only. Step 12 may successfully authorize the payment at the PSP. If the home region is then lost before step 13 receives the remote standby WAL-flush acknowledgement, both the local intent row and the asynchronous event-log replica may be missing. Recovery therefore cannot assume it knows the `order_id` / idempotency key.

Per-key lookup remains the normal path for an individual timeout or webhook. Regional disaster recovery needs a second operation that discovers unknown authorization IDs from the PSP ledger.

## Required PSP operations

### 1. Lookup when the key is known

```http
GET /charges/{idempotency_key}
```

Returns the current payment/authorization state and retained request metadata. A timeout remains `UNKNOWN`; it is not treated as a decline.

### 2. Discover authorization events when the key is unknown

```http
GET /ledger?from=<RFC3339>&to=<RFC3339>&event_type=AUTHORIZATION&cursor=<opaque>
```

`cursor` is omitted on the first request. Each returned cursor is bound to the `snapshot_id` and is passed unchanged on the next page request. `home_region` may be supplied as a server-side filter if the PSP supports it, but REC must also filter the returned metadata locally; correctness must not depend on an optional provider-side metadata filter.

Each page returns:

```json
{
  "snapshot_id": "stable-snapshot-token",
  "next_cursor": "opaque-next-page-token",
  "complete_through": "2026-11-22T10:15:00Z",
  "entries": [
    {
      "event_id": "psp-event-123",
      "charge_id": "psp-charge-456",
      "idempotency_key": "order-uuid",
      "event_type": "AUTHORIZATION",
      "status": "AUTHORIZED",
      "occurred_at": "2026-11-22T10:14:12Z",
      "metadata": {
        "order_id": "order-uuid",
        "item_id": "item-1",
        "unit_id": "unit-42",
        "user_id": "user-10",
        "home_region": "region-a",
        "fencing_epoch": 12
      }
    }
  ]
}
```

The exact timestamp values above are examples, not capacity or retention assumptions.

## Consistency and retention requirements

1. Pagination is over a stable snapshot. A cursor cannot silently skip entries or change the underlying result set while pages are read.
2. `complete_through` is a provider guarantee: all PSP ledger events with provider event time at or before that timestamp are queryable in the snapshot. A page being empty, or reaching the last page, is not by itself proof that the provider's index has caught up.
3. The provider retains the idempotency key, event/charge IDs, event type, status, event timestamp and full request metadata for at least the maximum regional recovery window defined by the service's DR policy.
4. Authorization discovery includes every authorization outcome needed for reconciliation (at minimum authorized, declined, voided and still-pending/unknown attempts), not just successful charges. The configured `authorization_settlement_grace` must exceed the PSP's documented maximum asynchronous authorization-processing delay plus clock-skew allowance. Any attempt still unresolved after that bound remains `UNKNOWN`; admissions do not reopen while an old-epoch authorization could still complete without being discoverable.
5. The provider supports an individual key lookup after discovery. Idempotency semantics and void/tombstone behaviour are documented and contract-tested.
6. If the provider cannot meet stable pagination, metadata retention and completeness-watermark requirements, it is not sufficient for autonomous regional write failover under the local-only step-11 intent design. Keep regional admissions/writes paused until an independently cross-region-durable pre-PSP attempt journal (or equivalent recovery mechanism) is deployed.

## Recovery checkpoint maintenance before a failure

REC also maintains the scan checkpoint during normal operation; it is not created only after disaster strikes. On a scheduled interval, REC scans the PSP authorization ledger from the previous `complete_through` minus the configured overlap through the newest provider-complete timestamp, reads every page under one stable snapshot, folds events idempotently, reconciles any newly discovered/changed attempts against PostgreSQL, and writes the new `complete_through` checkpoint in a PostgreSQL transaction configured for synchronous remote standby durability. The checkpoint advances only after every page is read and the related reconciliation is durable. If the sweep is interrupted, the previous checkpoint remains authoritative and the next sweep repeats the overlap. The checkpoint record includes the home region, snapshot ID, completed-through timestamp, scan bounds, and update epoch. Store it in a dedicated PostgreSQL table (for example, `psp_ledger_scan_checkpoints`) and commit each checkpoint with `synchronous_commit=on` so it is present on the remote standby. It is not stored only in the local-only step-11 intent or the asynchronous event log.

## Recovery algorithm

1. Fence the failed home-region writer and acquire a higher fencing epoch. Do not allow old-epoch workers to authorize, confirm, void or mutate inventory.
2. Load the last `complete_through` scan checkpoint that was committed with synchronous standby durability. If no checkpoint exists, scan from the earliest PSP-retained time that could include an in-flight authorization; if retention cannot cover that window, fail closed.
3. Choose a scan window from `last_complete_through - overlap` through `failover_detected_at + authorization_settlement_grace`. The settlement grace must cover the PSP's maximum in-flight authorization processing delay plus clock-skew allowance; old-epoch workers are fenced so they cannot start new calls. The overlap handles a crash between a completed scan and durable checkpoint advancement. The overlap and settlement grace are explicit DR configuration, not guessed constants.
4. Read every page under the same `snapshot_id`. Filter entries by `metadata.home_region` and the relevant fencing epoch; retain all candidate idempotency keys and event records. Deduplicate overlapping windows/pages by both `event_id` and `idempotency_key`, then fold events in provider event order to determine current state.
5. Do not call the scan complete until the provider's `complete_through` watermark covers the scan end and every page has been read successfully. If the cursor expires, the API errors, metadata is incomplete, or the watermark is behind, restart/continue the scan and keep admissions paused.
6. For every discovered ID and every order ID found in the promoted standby, perform `GET /charges/{idempotency_key}`. Check whether a durable step-13 `CONFIRMED` commit exists in the promoted standby. A PSP authorization alone is never proof of customer-visible confirmation.
7. Reconstruct idempotent recovery records for discovered attempts. Preserve an order only when the durable remote `CONFIRMED` commit can be proven. Void every authorization that has no such proof; cancellation/release is allowed only after a definitive void/decline result. Keep ambiguous attempts in `UNKNOWN` / `PAYING` and do not release their units.
8. Reconcile PostgreSQL, PSP ledger and inventory ownership. Persist the new scan checkpoint with synchronous standby durability only after the scan is complete and reconciliation has succeeded. Reopen admissions only after this gate is satisfied.

## Required mock-PSP and reconciler tests

- An authorization is recorded, its HTTP response is lost, and REC discovers it by ledger scan without being given the idempotency key.
- A local order intent and asynchronous Kafka record are absent after simulated home-region loss; ledger discovery still finds the authorization and all required metadata.
- Multiple pages under one snapshot return every matching event exactly once; cursor restart/replay is safe.
- Overlapping recovery windows deduplicate by PSP event ID and idempotency key.
- A delayed ledger-indexing fault holds `complete_through` behind the scan end; REC must not declare completion or reopen admissions.
- An authorization that completes late but within the configured settlement grace is found by the recovery scan; an unresolved old-epoch attempt beyond the provider's finality bound keeps admissions paused.
- A missing metadata field, expired cursor, provider error or insufficient retention fails closed.
- A durable step-13 confirmation found on the standby is preserved; an authorization without proof of durable confirmation is voided idempotently.
- Normal periodic checkpoint maintenance persists `complete_through` to the remote standby only after a stable-snapshot scan and reconciliation complete.
- A crash before the checkpoint commit causes the next run to rescan the overlap without duplicating effects.

## Implementation status

The current tree contains `prototype/cmd/mockpsp` and an implemented `/ledger` endpoint with stable snapshot pagination, metadata retention, event filters and a `complete_through` watermark. Mock PSP unit tests cover the contract. `RecoveryScanner` and PostgreSQL checkpoint persistence are implemented and have unit tests. Full integration against Docker Compose PostgreSQL, Redis Cluster and the mock PSP has not been executed in this environment; do not claim that distributed failover is proven until those tests run.
