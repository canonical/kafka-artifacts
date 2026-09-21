"""Smoke test against an installed, running snap: creates a topic, produces to it and reads the records back through the aliased upstream tools."""

import subprocess

import pytest

SNAP = "kafka"
BOOTSTRAP = "localhost:9092"
TOPIC = "smoke"
NUM_OPS = 100


def run(cmd, **kwargs):
    result = subprocess.run(cmd, text=True, capture_output=True, **kwargs)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result.stdout


def tool(name, *args, **kwargs):
    return run(["sudo", "snap", "run", f"{SNAP}.{name}", *args], **kwargs)


def test_kafka_snap_installed():
    result = subprocess.run(["snap", "list", SNAP], capture_output=True, text=True)
    assert result.returncode == 0, (
        f"'{SNAP}' snap is not installed.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.mark.run(after="test_kafka_snap_installed")
def test_broker_answers_api_versions():
    """The broker is up and serving the Kafka protocol on localhost:9092."""
    output = tool("kafka-broker-api-versions", "--bootstrap-server", BOOTSTRAP)
    assert BOOTSTRAP in output


@pytest.mark.run(after="test_broker_answers_api_versions")
def test_create_topic():
    # Re-runnable: the CI job runs this suite again after a snap refresh.
    tool(
        "kafka-topics",
        "--bootstrap-server", BOOTSTRAP,
        "--create", "--if-not-exists",
        "--topic", TOPIC,
        "--partitions", "1",
        "--replication-factor", "1",
    )
    listing = tool("kafka-topics", "--bootstrap-server", BOOTSTRAP, "--list")
    assert TOPIC in listing


@pytest.mark.run(after="test_create_topic")
def test_produce():
    payload = "\n".join(f"message-{i}" for i in range(NUM_OPS)) + "\n"
    tool(
        "kafka-console-producer",
        "--bootstrap-server", BOOTSTRAP,
        "--topic", TOPIC,
        input=payload,
    )


@pytest.mark.run(after="test_produce")
def test_consume():
    output = tool(
        "kafka-console-consumer",
        "--bootstrap-server", BOOTSTRAP,
        "--topic", TOPIC,
        "--from-beginning",
        "--max-messages", str(NUM_OPS),
        "--timeout-ms", "30000",
    )
    assert output.count("message-") >= NUM_OPS
