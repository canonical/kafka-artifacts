# Apache Kafka snap

[![Snapcraft Badge](https://snapcraft.io/kafka/badge.svg)](https://snapcraft.io/kafka)
[![Build and Test](https://github.com/canonical/kafka-snap/actions/workflows/ci.yaml/badge.svg)](https://github.com/canonical/kafka-snap/actions/workflows/ci.yaml)

A slim, standalone snap for [Apache Kafka](https://kafka.apache.org) — the
distributed event streaming platform.

## Install

```bash
sudo snap install kafka --channel 4/edge
```

The snap publishes to the `4/edge` track, following the latest upstream major
version.

Out of the box the snap ships a working single-node broker. On install
it seeds its configuration, and on first start it generates a cluster UUID and
formats the storage automatically.

The broker is then reachable on `localhost:9092`.

### Usage

Every bin-command tool is aliased to its upstream name:

```bash
kafka-topics --create --topic quickstart-events --bootstrap-server localhost:9092
kafka-topics --describe --topic quickstart-events --bootstrap-server localhost:9092

kafka-console-producer --topic quickstart-events --bootstrap-server localhost:9092

# write some lines, then Ctrl-C to exit

kafka-console-consumer --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

Both the aliased command (`kafka-topics ...`) and the full snap app
(`snap run kafka.kafka-topics ...`) work.

## Directory layout

Strict confinement forbids writing under `$SNAP`, so writable state lives under
`$SNAP_COMMON`:

| Purpose                    | Directory                                  |
| :------------------------- | :----------------------------------------- |
| Install root (bin, libs)   | `$SNAP/opt/kafka`                          |
| Upstream default configs   | `$SNAP/opt/kafka/config` (read-only)       |
| User config                | `$SNAP_COMMON/etc/kafka`                   |
| TLS secrets                | `$SNAP_COMMON/etc/kafka/secrets`           |
| Data / `log.dirs`          | `$SNAP_COMMON/var/lib/kafka/data`          |
| Service logs               | `$SNAP_COMMON/var/log/kafka`               |
| Connect plugin path        | `$SNAP_COMMON/var/lib/kafka/plugins`       |
| KRaft cluster-id marker    | `$SNAP_COMMON/etc/kafka/cluster.id`        |

The rendered config files, seeded from the upstream defaults on first install,
are `$SNAP_COMMON/etc/kafka/{server,connect-distributed,connect-standalone}.properties`
and the `log4j2.yaml` / `tools-log4j2.yaml` logging configs.

### Dedicated storage for Kafka data

To keep Kafka's log segments on a separate disk, mount it **onto the snap's data
directory**. That path already sits under `$SNAP_COMMON`, so strict confinement
allows the broker to write there with no interface to connect and no `log-dirs`
change — whatever is mounted at the path inherits the same access:

```bash
sudo systemctl stop snap.kafka.server            # stop the broker first
sudo mount /dev/sdb1 /var/snap/kafka/common/var/lib/kafka/data
sudo chown -R _daemon_:root /var/snap/kafka/common/var/lib/kafka/data
sudo systemctl start snap.kafka.server
```

The mount shadows the directory created on install, so re-`chown` it to
`_daemon_` **after** mounting. Add an `/etc/fstab` entry so it re-mounts on
boot, ordered before the Kafka service starts, to avoid the broker writing to the
underlying root disk while the mount is missing.

## Configuration

There are two ways to configure the broker, either through `snap set` or via 
manually editing `server.properties`. Both options can be used and mixed.

**NOTE - Config changes need a manual restart.** Neither `snap set` nor a manual
edit restarts the broker. Kafka reads `server.properties` only at startup, and the
snap deliberately does not restart a running broker automatically — an unplanned
restart of a stateful broker is disruptive, and in a cluster it is something
that should be applied deliberately. Apply changes with:

```bash
sudo snap restart kafka.server
```

`snap set` will not prompt to do this. The configure hook logs a reminder,
but snapd discards hook output on a successful `snap set`.

### 1. `snap set` options

Common `server.properties` keys are settable through `snap set kafka <key>=<value>`
and merged into the rendered config. Snap options use the property key with `.`
rewritten to `-`:

```bash
sudo snap set kafka node-id=1
sudo snap set kafka process-roles=broker,controller
sudo snap set kafka listeners=PLAINTEXT://:9092,CONTROLLER://:9093
sudo snap set kafka log-dirs=/var/snap/kafka/common/var/lib/kafka/data
sudo snap restart kafka.server   # the broker reads server.properties at startup
```

Supported options and the `server.properties` key each maps to:

| Snap option                                   | `server.properties` key                       |
| :-------------------------------------------- | :-------------------------------------------- |
| `node-id`                                     | `node.id`                                     |
| `process-roles`                               | `process.roles`                               |
| `listeners`                                   | `listeners`                                   |
| `advertised-listeners`                        | `advertised.listeners`                        |
| `controller-quorum-voters`                    | `controller.quorum.voters`                    |
| `controller-listener-names`                   | `controller.listener.names`                   |
| `inter-broker-listener-name`                  | `inter.broker.listener.name`                  |
| `listener-security-protocol-map`              | `listener.security.protocol.map`              |
| `log-dirs`                                    | `log.dirs`                                    |
| `num-partitions`                              | `num.partitions`                              |
| `default-replication-factor`                  | `default.replication.factor`                  |
| `offsets-topic-replication-factor`            | `offsets.topic.replication.factor`            |
| `transaction-state-log-replication-factor`    | `transaction.state.log.replication.factor`    |
| `transaction-state-log-min-isr`               | `transaction.state.log.min.isr`               |
| `auto-create-topics-enable`                   | `auto.create.topics.enable`                   |

The cluster UUID is set with `snap set kafka cluster-id=<uuid>` and is used
only before the storage is first formatted.

Only the options in the table above are able to be set with `snap set`.

A key that ships in the default `server.properties` but isn't mapped (e.g. 
`num-network-threads`) is **rejected** — `snap set` fails and tells you to set
it in the file instead. Any other key, for example `ssl-truststore-location`
will be stored by snapd and silently ignored.

To set properties not in the table, edit `server.properties` directly (see below).

### 2. Editing the config directly

Edit `$SNAP_COMMON/etc/kafka/server.properties` (that is
`/var/snap/kafka/common/etc/kafka/server.properties`) and restart the broker.
Manually set configuration takes precedence over the matching `snap set` option,
the configure hook will not overwrite it.

Custom log4j2 config goes in `$SNAP_COMMON/etc/kafka/log4j2.yaml`
(`tools-log4j2.yaml` for the CLI tools).

### Multi-node cluster

To cluster several nodes, run each as a combined broker+controller that shares one
`cluster-id` and a static voter set. Because the snap auto-formats a single node on
install, first `snap stop kafka.server` and clear
`$SNAP_COMMON/var/lib/kafka/data` and `.../etc/kafka/cluster.id` on each node, then
generate one UUID with `kafka-storage random-uuid` and, on every node `N`, apply the
shared config and start:

```bash
sudo snap set kafka cluster-id=<shared-uuid> node-id=<N> \
  process-roles=broker,controller advertised-listeners=PLAINTEXT://<this-node-ip>:9092 \
  controller-quorum-voters=1@<ip1>:9093,2@<ip2>:9093,3@<ip3>:9093

sudo sed -i '/^controller.quorum.bootstrap.servers=/d' \
  /var/snap/kafka/common/etc/kafka/server.properties   # static voters replace dynamic bootstrap

sudo snap start kafka.server
```

## Apps

`kafka.server` is the broker daemon (KRaft combined broker + controller in the
default single-node config). Every upstream `bin/*.sh` tool is exposed as a snap
app named after the script basename with `.sh` stripped, and aliased to that
same name:

```
kafka-acls                    kafka-jmx                     kafka-storage
kafka-broker-api-versions     kafka-leader-election         kafka-streams-application-reset
kafka-client-metrics          kafka-log-dirs                kafka-topics
kafka-cluster                 kafka-metadata-quorum         kafka-transactions
kafka-configs                 kafka-metadata-shell          kafka-verifiable-consumer
kafka-console-consumer        kafka-producer-perf-test      kafka-verifiable-producer
kafka-console-producer        kafka-reassign-partitions     trogdor
kafka-consumer-groups         kafka-replica-verification
kafka-consumer-perf-test      kafka-run-class
kafka-delegation-tokens       kafka-delete-records
kafka-dump-log                kafka-e2e-latency
kafka-features                kafka-get-offsets
```

### Kafka Connect

| App                    | Type            | Notes                                                  |
| :--------------------- | :-------------- | :----------------------------------------------------- |
| `connect-distributed`  | daemon          | Reads `$SNAP_COMMON/etc/kafka/connect-distributed.properties`. `snap start kafka.connect-distributed`. |
| `connect-standalone`   | one-shot app    | Takes worker + connector config files as arguments.    |
| `connect-mirror-maker` | one-shot app    | MirrorMaker 2.                                          |
| `connect-plugin-path`  | app             | Manage the connector plugin path.                      |

Connector plugins are loaded from `$SNAP_COMMON/var/lib/kafka/plugins`, where they
can be set manually.

### Utilities

| App       | Notes                                            |
| :-------- | :----------------------------------------------- |
| `keytool` | The bundled JRE `keytool`, for TLS keystores.    |

## Confinement and interfaces

`confinement: strict`, `grade: stable`, `base: core26`, `platforms: [amd64, arm64]`.

Interfaces: `network`, `network-bind`, `mount-observe` on the daemons and tools;
`removable-media` on the broker (for an external `log.dirs`); `home` on the tools
(to read client property files / keystores from `$HOME`). `network` and
`network-bind` auto-connect; connect the rest if you need them:

```bash
sudo snap connect kafka:mount-observe
sudo snap connect kafka:removable-media
sudo snap connect kafka:home
```

## Building

```bash
snapcraft --debug
sudo snap install ./kafka_*.snap --dangerous
```

The Apache Kafka binaries come from the
[`central-uploader`](https://github.com/canonical/central-uploader) release for
the packaged version; the tarball URL is resolved at build time.

## Testing

Tests run against an installed snap, driven through `just`:

```bash
just smoke    # broker comes up, a topic round-trips messages
just config   # snap set / hand-edit rendering onto server.properties
just cluster  # 3-node LXD quorum, failover and recovery (KEEP=1 to inspect after)
just lint     # yamllint
```

`snapcraft test` runs the same suites in a VM via `spread` (see `spread.yaml`).

## License

The snap packaging is licensed under the Apache License 2.0 — see
[LICENSE](./LICENSE). Apache Kafka is a trademark of the Apache Software
Foundation.
