"""Config tests against an installed snap: every case goes through "snap set"/"snap unset" (covering snapd's rollback too) and restores the snap afterwards."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

SNAP = "kafka"
SNAP_PATH = Path("/snap/kafka/current")
SNAP_COMMON = Path("/var/snap/kafka/common")

RENDER_CONFIG = SNAP_PATH / "opt" / "shared" / "bin" / "render-config.sh"
SHIPPED_CONF = SNAP_PATH / "opt" / "kafka" / "config" / "server.properties"
LIVE_CONF = SNAP_COMMON / "etc" / "kafka" / "server.properties"
LIVE_STATE = SNAP_COMMON / "ops" / "rendered-config.properties"
LIVE_LOG = SNAP_COMMON / "ops" / "snap" / "logs" / "hook-configure.log"

# Values for the whole mapping; never applied to a running broker, so they only need to be plausible and distinct from the defaults.
OPTION_VALUES = {
    "node-id": "1",
    "process-roles": "broker,controller",
    "listeners": "PLAINTEXT://:9092,CONTROLLER://:9093",
    "advertised-listeners": "PLAINTEXT://10.0.0.5:9092",
    "controller-quorum-voters": "1@10.0.0.5:9093",
    "controller-listener-names": "CONTROLLER",
    "inter-broker-listener-name": "PLAINTEXT",
    "listener-security-protocol-map": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "log-dirs": "/var/snap/kafka/common/var/lib/kafka/data",
    "num-partitions": "3",
    "default-replication-factor": "1",
    "offsets-topic-replication-factor": "1",
    "transaction-state-log-replication-factor": "1",
    "transaction-state-log-min-isr": "1",
    "auto-create-topics-enable": "false",
}


def snap_installed():
    if not shutil.which("snap"):
        return False
    return subprocess.run(["snap", "list", SNAP], capture_output=True).returncode == 0


def root_available():
    return subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0


pytestmark = [
    pytest.mark.skipif(
        not snap_installed(), reason=f"the {SNAP} snap is not installed"
    ),
    pytest.mark.skipif(
        not root_available(),
        reason="needs passwordless sudo: snap get/set and server.properties are root owned",
    ),
]


def snap_get():
    """Every option currently set on the snap."""
    result = subprocess.run(
        ["sudo", "-n", "snap", "get", SNAP, "-d"], capture_output=True, text=True
    )
    if result.returncode != 0:
        # snapd fails rather than printing {} when nothing is set
        return {}
    return json.loads(result.stdout)


def snap_set(*assignments):
    return subprocess.run(
        ["sudo", "-n", "snap", "set", SNAP, *assignments],
        capture_output=True,
        text=True,
    )


def snap_unset(*options):
    return subprocess.run(
        ["sudo", "-n", "snap", "unset", SNAP, *options], capture_output=True, text=True
    )


def write_as_root(path, content):
    subprocess.run(
        ["sudo", "-n", "tee", str(path)],
        input=content,
        stdout=subprocess.DEVNULL,
        check=True,
    )


def read_as_root(path) -> bytes:
    """Reads a root-owned file the way write_as_root writes one; config is 770 _daemon_:root."""
    return subprocess.run(
        ["sudo", "-n", "cat", str(path)], capture_output=True, check=True
    ).stdout


def exists_as_root(path) -> bool:
    return subprocess.run(["sudo", "-n", "test", "-e", str(path)]).returncode == 0


def config_options():
    """The option -> key mapping the installed snap declares."""
    body = re.search(
        r"declare -A CONFIG_OPTIONS=\((.*?)\n\)", RENDER_CONFIG.read_text(), re.DOTALL
    )
    assert body, f"CONFIG_OPTIONS not found in {RENDER_CONFIG}"
    return dict(re.findall(r"\[(\S+)\]=\"([^\"]+)\"", body.group(1)))


def read_prop(text, key):
    """The value assigned to a properties key, or None. Ignores commented lines."""
    value = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        candidate, _, rest = stripped.partition("=")
        if candidate.strip() == key:
            value = rest
    return value


def rendered():
    return read_as_root(LIVE_CONF).decode()


def recorded_state():
    if not exists_as_root(LIVE_STATE):
        return ""
    return read_as_root(LIVE_STATE).decode()


def log_lines():
    if not exists_as_root(LIVE_LOG):
        return []
    return read_as_root(LIVE_LOG).decode().splitlines()


def edit_conf(key, value):
    """Stands in for an administrator editing server.properties by hand."""
    lines = read_as_root(LIVE_CONF).decode().splitlines()
    out, done = [], False
    for line in lines:
        stripped = line.lstrip("#").strip()
        if not done and "=" in stripped and stripped.split("=", 1)[0].strip() == key:
            out.append(f"{key}={value}")
            done = True
        else:
            out.append(line)
    if not done:
        out.append(f"{key}={value}")
    write_as_root(LIVE_CONF, ("\n".join(out) + "\n").encode())


@pytest.fixture(autouse=True)
def restored():
    """Puts the snap back exactly as it was found (server.properties restored before and after the options)."""
    options = snap_get()
    conf = read_as_root(LIVE_CONF)
    state = read_as_root(LIVE_STATE) if exists_as_root(LIVE_STATE) else None

    def restore_files():
        write_as_root(LIVE_CONF, conf)
        if state is None:
            subprocess.run(["sudo", "-n", "rm", "-f", str(LIVE_STATE)], check=True)
        else:
            write_as_root(LIVE_STATE, state)

    yield

    restore_files()

    added = set(snap_get()) - set(options)
    if added:
        snap_unset(*added)
    if options:
        snap_set(*(f"{key}={value}" for key, value in options.items()))

    restore_files()


def test_set_option_reaches_server_properties():
    result = snap_set("num-partitions=3")

    assert result.returncode == 0, result.stderr
    assert read_prop(rendered(), "num.partitions") == "3"
    assert read_prop(recorded_state(), "num-partitions") == "3"


def test_every_supported_option_is_rendered():
    """Guards the whole mapping, not just the key the other tests use."""
    mapping = config_options()
    assert set(OPTION_VALUES) == set(mapping), "test data is out of step with the snap"

    result = snap_set(*(f"{option}={value}" for option, value in OPTION_VALUES.items()))

    assert result.returncode == 0, result.stderr

    document = rendered()
    for option, value in OPTION_VALUES.items():
        assert (
            read_prop(document, mapping[option]) == value
        ), f"{option} was not rendered"


def test_setting_the_same_value_again_changes_nothing():
    """The hook also runs on refresh, where it must not ask for a restart."""
    assert snap_set("num-partitions=3").returncode == 0
    before = log_lines()

    assert snap_set("num-partitions=3").returncode == 0

    new_lines = log_lines()[len(before) :]
    assert not [line for line in new_lines if "setting num.partitions" in line]
    assert not [line for line in new_lines if "snap restart" in line]


def test_unsupported_option_is_rejected_and_rolled_back():
    """snapd must undo an option whose configure hook failed; "num-network-threads" is a real key left out of the mapping so the hook can refuse it."""
    result = snap_set("num-network-threads=4")

    assert result.returncode != 0
    assert "num-network-threads" in result.stderr
    assert "num-network-threads" not in snap_get()


def test_unsupported_option_is_rejected_before_writing():
    before = read_as_root(LIVE_CONF)

    result = snap_set("num-network-threads=4", "num-partitions=3")

    assert result.returncode != 0
    assert read_as_root(LIVE_CONF) == before


def test_option_naming_no_kafka_key_is_accepted():
    """A name matching no server.properties key is invisible to the hook, so snapd just stores it."""
    before = read_as_root(LIVE_CONF)

    result = snap_set("definitely-not-a-kafka-option=1")

    assert result.returncode == 0, result.stderr
    assert read_as_root(LIVE_CONF) == before


def test_hand_edit_blocks_the_option():
    """The case that has to fail: a "snap set" that cannot be honoured."""
    edit_conf("num.partitions", "7")

    result = snap_set("num-partitions=3")

    assert result.returncode != 0
    assert "cannot apply the 'num-partitions' option" in result.stderr
    assert read_prop(rendered(), "num.partitions") == "7"
    assert "num-partitions" not in snap_get()


def test_hand_edit_of_a_rendered_key_is_kept():
    """Editing a key the snap already wrote is allowed and survives; re-running the hook here (the refresh path) must not fail."""
    assert snap_set("num-partitions=3").returncode == 0
    edit_conf("num.partitions", "9")
    before = log_lines()

    # an unrelated option, to make snapd run the hook again
    result = snap_set("default-replication-factor=1")

    assert result.returncode == 0, result.stderr
    assert read_prop(rendered(), "num.partitions") == "9"

    new_lines = "\n".join(log_lines()[len(before) :])
    assert "ignoring the 'num-partitions' option" in new_lines


def test_manual_edit_wins_over_prior_set_but_later_set_still_edits_the_file():
    """A hand edit overrides an earlier "snap set" of that key, yet a later "snap set" of another option still writes the file, leaving the edit intact."""
    assert snap_set("num-partitions=3").returncode == 0
    edit_conf("num.partitions", "99")

    result = snap_set("default-replication-factor=2")

    assert result.returncode == 0, result.stderr
    assert read_prop(rendered(), "default.replication.factor") == "2"
    assert read_prop(rendered(), "num.partitions") == "99"


def test_unset_restores_the_shipped_default():
    shipped = read_prop(SHIPPED_CONF.read_text(), "num.partitions")
    assert snap_set("num-partitions=3").returncode == 0

    result = snap_unset("num-partitions")

    assert result.returncode == 0, result.stderr
    assert read_prop(rendered(), "num.partitions") == shipped
    assert read_prop(recorded_state(), "num-partitions") is None


def test_unset_leaves_a_hand_edit_alone():
    assert snap_set("num-partitions=3").returncode == 0
    edit_conf("num.partitions", "11")

    result = snap_unset("num-partitions")

    assert result.returncode == 0, result.stderr
    assert read_prop(rendered(), "num.partitions") == "11"


def test_configure_log_records_the_changes():
    """snapd discards hook output on success, so the log is the only record."""
    before = len(log_lines())

    assert snap_set("num-partitions=3").returncode == 0
    assert snap_set("num-partitions=5").returncode == 0

    logged = "\n".join(log_lines()[before:])

    assert "setting num.partitions to '3'" in logged
    assert "setting num.partitions to '5'" in logged
    assert "run 'snap restart kafka.server'" in logged


def test_readme_documents_every_option():
    """The option table in the README is the contract, so it has to be right."""
    readme = (REPO / "README.md").read_text()
    for option in config_options():
        assert f"`{option}`" in readme, f"{option} is not documented in the README"
