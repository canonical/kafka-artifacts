"""Multi-node cluster test: launches 3 LXD instances, installs the snap on each as a
combined KRaft controller + broker, forms a static-quorum cluster, and checks health,
quorum, read/write, node-down failover, and node recovery from the disk.

A session fixture builds and tears down the cluster; each phase is an ordered test.
KEEP=1 leaves the instances up, TYPE=container uses containers instead of VMs, and
KAFKA_SNAP (or the newest kafka_*.snap in the repo root) is the snap under test.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

REPO = Path(__file__).resolve().parents[2]

SNAP = "kafka"
NODES = ["kafka-1", "kafka-2", "kafka-3"]
TOPIC = "cluster-smoke"
BATCH = 200
DATA_DIR = "/var/snap/kafka/common/var/lib/kafka/data"
SERVER_PROPS = "/var/snap/kafka/common/etc/kafka/server.properties"
CLUSTER_ID_FILE = "/var/snap/kafka/common/etc/kafka/cluster.id"

IMAGE = os.environ.get("IMAGE", "ubuntu:24.04")
TYPE = os.environ.get("TYPE", "vm")
KEEP = os.environ.get("KEEP", "")


def find_snap() -> Path | None:
    env = os.environ.get("KAFKA_SNAP")
    if env:
        return Path(env)

    built = sorted(
        REPO.glob("kafka_*.snap"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    return built[0] if built else None


SNAP_FILE = find_snap()

pytestmark = [
    pytest.mark.skipif(
        shutil.which("lxc") is None, reason="lxc (LXD) is required for the cluster test"
    ),
    pytest.mark.skipif(
        SNAP_FILE is None or not SNAP_FILE.is_file(),
        reason="snap not found; build one (snapcraft pack) or set KAFKA_SNAP",
    ),
]


def retrying(seconds: int) -> Retrying:
    """Polls a `with attempt` block every 3s until it stops raising, or the timeout elapses."""
    return Retrying(
        stop=stop_after_delay(seconds),
        wait=wait_fixed(3),
        reraise=True,
        retry=retry_if_exception_type((AssertionError, subprocess.CalledProcessError)),
    )


def lxc(*args, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(["lxc", *args], text=True, capture_output=True)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, args, result.stdout, result.stderr
        )
    return result


class Node:
    """A single LXD instance: launches it, runs commands in it, installs snaps."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.ip = ""

    def run(self, *cmd, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
        """Run a command inside the instance."""
        result = subprocess.run(
            ["lxc", "exec", self.name, "--", *cmd],
            text=True,
            capture_output=True,
            **kwargs,
        )
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )
        return result

    def launch(self) -> None:
        if lxc("info", self.name).returncode == 0:
            lxc("delete", "-f", self.name)

        flags = ["--vm"] if TYPE == "vm" else ["-c", "security.nesting=true"]
        lxc(
            "launch",
            IMAGE,
            self.name,
            *flags,
            "-c",
            "limits.memory=2GiB",
            "-c",
            "limits.cpu=2",
            check=True,
        )

    def wait_for_agent(self) -> None:
        for attempt in retrying(180):
            with attempt:
                assert (
                    self.run("true", check=False).returncode == 0
                ), f"{self.name} lxc agent not responding"

    def wait_for_snapd(self) -> None:
        for attempt in retrying(180):
            with attempt:
                seeded = self.run("snap", "wait", "system", "seed.loaded", check=False)
                assert seeded.returncode == 0, f"{self.name} snapd not seeded"

    def current_ip(self) -> str:
        return self.run(
            "bash", "-c", "hostname -I | awk '{print $1}'", check=False
        ).stdout.strip()

    def resolve_ip(self) -> str:
        for attempt in retrying(60):
            with attempt:
                self.ip = self.current_ip()
                assert self.ip, f"no IP for {self.name}"

        return self.ip

    def push_file(self, local: Path, remote: str) -> None:
        lxc("file", "push", str(local), f"{self.name}{remote}", check=True)

    def install_snap(
        self, snap: str, dangerous: bool = False, check: bool = True
    ) -> None:
        args = ["snap", "install", snap]
        if dangerous:
            args.append("--dangerous")
        self.run(*args, check=check)

    def stop(self) -> None:
        lxc("stop", self.name, check=True)

    def start(self) -> None:
        lxc("start", self.name, check=True)

    def delete(self) -> None:
        lxc("delete", "-f", self.name)


class Kafka:
    """The Kafka commands, run through the snap on a given node."""

    def __init__(self, node: Node) -> None:
        self.node = node

    @property
    def bootstrap(self) -> str:
        return f"{self.node.ip}:9092"

    def _run(self, app: str, *args, **kwargs) -> subprocess.CompletedProcess:
        return self.node.run("snap", "run", f"{SNAP}.{app}", *args, **kwargs)

    def random_cluster_id(self) -> str:
        return self._run("kafka-storage", "random-uuid").stdout.splitlines()[-1].strip()

    def broker_ready(self) -> bool:
        return (
            self._run(
                "kafka-broker-api-versions",
                "--bootstrap-server",
                self.bootstrap,
                check=False,
            ).returncode
            == 0
        )

    def create_topic(
        self, topic: str, partitions: int, replication_factor: int
    ) -> None:
        self._run(
            "kafka-topics",
            "--bootstrap-server",
            self.bootstrap,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            str(replication_factor),
        )

    def min_isr(self, topic: str) -> int | None:
        """Minimum in-sync-replica count across the topic's partitions, or None."""
        out = self._run(
            "kafka-topics",
            "--bootstrap-server",
            self.bootstrap,
            "--describe",
            "--topic",
            topic,
        ).stdout
        counts = [len(isr.split(",")) for isr in re.findall(r"Isr: ([0-9,]+)", out)]
        return min(counts) if counts else None

    def isr_is(self, topic: str, count: int) -> bool:
        return self.min_isr(topic=topic) == count

    def isr_at_most(self, topic: str, count: int) -> bool:
        value = self.min_isr(topic=topic)
        return value is not None and value <= count

    def quorum_leader(self) -> int | None:
        """The elected KRaft controller leader id, or None when there is no quorum."""
        out = self._run(
            "kafka-metadata-quorum",
            "--bootstrap-server",
            self.bootstrap,
            "describe",
            "--status",
            check=False,
        ).stdout
        match = re.search(r"LeaderId:\s*([0-9]+)", out)
        return int(match.group(1)) if match else None

    def voter_count(self) -> int:
        out = self._run(
            "kafka-metadata-quorum",
            "--bootstrap-server",
            self.bootstrap,
            "describe",
            "--replication",
        ).stdout
        ids = {
            m.group(0) for line in out.splitlines() if (m := re.match(r"[0-9]+", line))
        }
        return len(ids)

    def produce(self, topic: str, phase: str, count: int) -> None:
        payload = "".join(f"msg-{phase}-{i}\n" for i in range(1, count + 1))
        self._run(
            "kafka-console-producer",
            "--bootstrap-server",
            self.bootstrap,
            "--topic",
            topic,
            input=payload,
        )

    def consume_count(self, topic: str, want: int) -> int:
        """Count of records read from the beginning, up to want."""
        out = self._run(
            "kafka-console-consumer",
            "--bootstrap-server",
            self.bootstrap,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            str(want),
            "--timeout-ms",
            "60000",
            check=False,
        ).stdout
        return sum(1 for line in out.splitlines() if line.startswith("msg-"))

    def apply_member_config(self, node_id: int, cluster_id: str, voters: str) -> None:
        """Point this node at a shared cluster id and the static controller voters."""
        self.node.run(
            "snap",
            "set",
            "kafka",
            f"cluster-id={cluster_id}",
            f"node-id={node_id}",
            "process-roles=broker,controller",
            "listeners=PLAINTEXT://:9092,CONTROLLER://:9093",
            f"advertised-listeners=PLAINTEXT://{self.node.ip}:9092",
            "controller-listener-names=CONTROLLER",
            "inter-broker-listener-name=PLAINTEXT",
            "listener-security-protocol-map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            f"controller-quorum-voters={voters}",
            "offsets-topic-replication-factor=3",
            "transaction-state-log-replication-factor=3",
            "transaction-state-log-min-isr=2",
            "default-replication-factor=3",
            f"log-dirs={DATA_DIR}",
        )


class LXDCluster:
    """The LXD instances that make up the cluster, and the static-quorum formation."""

    def __init__(self, names: list[str]) -> None:
        self.nodes = [Node(name=name) for name in names]

    def node(self, name: str) -> Node:
        return next(node for node in self.nodes if node.name == name)

    def kafka(self, name: str) -> Kafka:
        return Kafka(node=self.node(name=name))

    def launch(self, snap_file: Path) -> None:
        for node in self.nodes:
            node.launch()

        for node in self.nodes:
            node.wait_for_agent()
            node.wait_for_snapd()
            node.resolve_ip()

            # the base fetch hits the store, so retry it; a swallowed failure here only
            # resurfaces later as a confusing "kafka.snap install failed" (missing base)
            for attempt in retrying(180):
                with attempt:
                    node.install_snap(snap="core26")

            node.push_file(local=snap_file, remote="/root/kafka.snap")
            node.install_snap(snap="/root/kafka.snap", dangerous=True)

    def configure(self) -> None:
        # The server auto-starts and formats a standalone node on install; stop it, wipe that
        # storage, apply the shared cluster id + static voters, then let it reformat and join.
        cluster_id = self.kafka(name="kafka-1").random_cluster_id()
        voters = ",".join(
            f"{i}@{self.node(name=f'kafka-{i}').ip}:9093" for i in (1, 2, 3)
        )

        for i in (1, 2, 3):
            node = self.node(name=f"kafka-{i}")

            node.run("snap", "stop", "kafka.server")
            node.run("rm", "-rf", DATA_DIR)
            node.run(
                "install", "-d", "-o", "_daemon_", "-g", "root", "-m", "750", DATA_DIR
            )
            node.run("rm", "-f", CLUSTER_ID_FILE)

            Kafka(node=node).apply_member_config(
                node_id=i, cluster_id=cluster_id, voters=voters
            )

            # Static voters replace the shipped dynamic bootstrap discovery.
            node.run(
                "sed", "-i", "/^controller.quorum.bootstrap.servers=/d", SERVER_PROPS
            )

        for node in self.nodes:
            node.run("snap", "start", "kafka.server")

    def destroy(self) -> None:
        if KEEP:
            return

        for node in self.nodes:
            node.delete()


@pytest.fixture(scope="session")
def cluster(request) -> LXDCluster:
    instance = LXDCluster(names=NODES)
    request.addfinalizer(instance.destroy)

    instance.launch(snap_file=SNAP_FILE)
    instance.configure()

    return instance


def test_services_active_and_brokers_answer(cluster: LXDCluster) -> None:
    """Services healthy and brokers answering."""
    for node in cluster.nodes:
        services = node.run("snap", "services", "kafka.server").stdout
        assert " active " in services, f"{node.name} server service not active"

        for attempt in retrying(120):
            with attempt:
                assert Kafka(
                    node=node
                ).broker_ready(), f"{node.name} broker not answering"


@pytest.mark.run(after="test_services_active_and_brokers_answer")
def test_kraft_quorum_has_leader_and_three_voters(cluster: LXDCluster) -> None:
    """KRaft quorum has a leader and 3 voters."""
    kafka = cluster.kafka(name="kafka-1")

    for attempt in retrying(60):
        with attempt:
            assert kafka.quorum_leader() is not None, "no KRaft leader elected"

    voters_seen = kafka.voter_count()
    assert voters_seen == 3, f"expected 3 controller replicas, saw {voters_seen}"


@pytest.mark.run(after="test_kraft_quorum_has_leader_and_three_voters")
def test_replicated_topic_round_trips(cluster: LXDCluster) -> None:
    """Create a replicated topic and round-trip records."""
    kafka1 = cluster.kafka(name="kafka-1")
    kafka2 = cluster.kafka(name="kafka-2")

    kafka1.create_topic(topic=TOPIC, partitions=3, replication_factor=3)
    for attempt in retrying(60):
        with attempt:
            assert kafka1.isr_is(topic=TOPIC, count=3), "topic never reached ISR=3"

    kafka1.produce(topic=TOPIC, phase="a", count=BATCH)
    got = kafka2.consume_count(topic=TOPIC, want=BATCH)
    assert got >= BATCH, f"expected >= {BATCH} records, got {got}"


@pytest.mark.run(after="test_replicated_topic_round_trips")
def test_survives_a_node_going_down(cluster: LXDCluster) -> None:
    """Stop kafka-3, cluster keeps serving."""
    kafka1 = cluster.kafka(name="kafka-1")
    kafka2 = cluster.kafka(name="kafka-2")

    cluster.node(name="kafka-3").stop()
    for attempt in retrying(60):
        with attempt:
            assert kafka1.isr_at_most(topic=TOPIC, count=2), "ISR did not shrink"

    assert kafka1.quorum_leader() is not None, "quorum lost a leader with one node down"
    isr = kafka1.min_isr(topic=TOPIC)
    assert isr is not None and isr >= 2, "ISR dropped below 2 with one node down"

    kafka1.produce(topic=TOPIC, phase="b", count=BATCH)
    got = kafka2.consume_count(topic=TOPIC, want=2 * BATCH)
    assert (
        got >= 2 * BATCH
    ), f"expected >= {2 * BATCH} records during failover, got {got}"


@pytest.mark.run(after="test_survives_a_node_going_down")
def test_recovers_from_disk_after_restart(cluster: LXDCluster) -> None:
    """Restart kafka-3, it recovers its disk and rejoins."""
    node3 = cluster.node(name="kafka-3")
    kafka1 = cluster.kafka(name="kafka-1")

    node3.start()
    node3.wait_for_agent()
    node3.resolve_ip()
    kafka3 = Kafka(node=node3)

    for attempt in retrying(180):
        with attempt:
            assert kafka3.broker_ready(), "kafka-3 broker not answering"

    for attempt in retrying(120):
        with attempt:
            assert kafka3.isr_is(
                topic=TOPIC, count=3
            ), "kafka-3 did not rejoin to ISR=3"

    recovered = kafka3.consume_count(topic=TOPIC, want=2 * BATCH)
    assert (
        recovered >= 2 * BATCH
    ), f"kafka-3 did not recover the existing data (got {recovered})"

    kafka3.produce(topic=TOPIC, phase="c", count=BATCH)
    got = kafka1.consume_count(topic=TOPIC, want=3 * BATCH)
    assert got >= 3 * BATCH, f"read/write broken after recovery (got {got})"
