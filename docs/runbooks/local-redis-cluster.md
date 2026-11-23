# Local Redis Cluster networking for integration tests

## Decision

Run Redis Cluster integration tests from a test-runner container attached to the same user-defined Docker Compose network as all three Redis primary nodes. Do not run Redis Cluster integration tests from the host in the default topology.

Redis Cluster clients follow `MOVED` / `ASK` redirections to the node endpoints returned by cluster metadata. A seed connection to a published host port is not sufficient if the advertised endpoint is a Docker-only container address. Redis also requires both the client port and the cluster-bus port to be reachable between cluster nodes. The default internal ports are `6379` and `16379` respectively. See the Redis Cluster networking documentation: https://redis.io/docs/latest/management/scaling/ and https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/.

## Compose topology contract

- Services: `redis-inventory-1`, `redis-inventory-2`, `redis-inventory-3`, `redis-ctrl`, `redis-kv`, `redis-cluster-init`, and `integration`.
- All inventory Redis nodes, `redis-cluster-init`, and `integration` join the same user-defined Compose network.
- Each inventory node has its own data/config volume; do not share `nodes.conf` between nodes.
- Expose client port `6379` and cluster-bus port `16379` to the Compose network. The cluster bus must be reachable between all inventory nodes.
- Do not publish/remap the inventory cluster ports to the host in the default profile. Keep host access for `redis-ctrl` and `redis-kv` separate if local debugging needs it.
- Use Redis 7.4+ and configure each node with its own Compose service name as its announced hostname, for example:

```conf
port 6379
cluster-enabled yes
cluster-config-file /data/nodes.conf
cluster-node-timeout 5000
appendonly yes
cluster-announce-hostname redis-inventory-1
cluster-preferred-endpoint-type hostname
cluster-announce-port 6379
cluster-announce-bus-port 16379
```

Repeat with `redis-inventory-2` and `redis-inventory-3` for the other nodes. Container-to-container cluster-bus connectivity uses the Compose network; cluster client endpoints returned to clients use the announced service hostnames. Verify this with `CLUSTER SHARDS` / `CLUSTER SLOTS` from the `integration` container before running tests.

Initialize the three-primary, no-replica local test cluster from `redis-cluster-init` using the three internal service endpoints, for example:

```sh
redis-cli --cluster create \
  redis-inventory-1:6379 \
  redis-inventory-2:6379 \
  redis-inventory-3:6379 \
  --cluster-replicas 0 --cluster-yes
```

This is a slot-distribution test topology, not an HA production cluster. The local cluster has three primaries and no replicas; it must not be used to claim failover durability.

## Test execution

Pure Go unit tests may run on the host. Redis Cluster integration tests run in the network-attached test runner:

```sh
docker compose run --rm integration \
  go test -p 1 -parallel 1 ./tests/integration/...
```

The test runner's Redis seed addresses are `redis-inventory-1:6379`, `redis-inventory-2:6379`, and `redis-inventory-3:6379`. Because the runner shares the network, every announced endpoint in a cluster redirection is reachable. Shared-service integration tests must not use `t.Parallel`.

## Optional host-access profile

If host-side Redis Cluster integration testing is ever required, do not merely publish three container ports while leaving the nodes advertising their internal addresses. Every node must advertise a distinct reachable host IP, client port, and matching cluster-bus port, and all nodes must be configured consistently for that topology. A host-network profile with unique ports may be used for local-only debugging, but it is not the default or the CI path. Never make every node advertise the same `127.0.0.1:6379` endpoint.

## Acceptance checks

1. From `integration`, connect to the first seed and run `CLUSTER SHARDS` (or `CLUSTER SLOTS`).
2. Confirm each returned endpoint resolves from the `integration` container.
3. Execute a command that touches keys in multiple slots and verify the client follows `MOVED` to another node.
4. Verify all three client ports and all three cluster-bus ports are reachable on the Compose network.
5. Run the integration package sequentially with `-p 1 -parallel 1`.
