# Recipes for building, testing and operating the snap. "just" lists them.

pytest := "poetry run pytest -vv --no-header --tb native --log-cli-level=INFO"

[private]
default:
    @just --list

# Check the packaging against the coding style standards
lint: lint-deps
    poetry run yamllint --no-warnings ./snap/ spread.yaml tests/spread/ .github/

# Configuration tests, against an installed snap
config: test-deps
    {{ pytest }} tests/test_config.py -s

# Smoke test, against an installed and running snap
smoke: test-deps
    {{ pytest }} tests/test_smoke.py -s

# Multi-node cluster test: 3 LXD instances, quorum, failover and recovery (KEEP=1 to inspect after)
cluster snap="": test-deps
    KAFKA_SNAP="{{ snap }}" {{ pytest }} tests/cluster/test_cluster.py -s

# Connect the interfaces the broker declares but that do not auto-connect
connect-interfaces:
    sudo snap connect kafka:mount-observe
    sudo snap connect kafka:removable-media
    sudo snap connect kafka:home

# Declared as recipe dependencies so "just config smoke" installs them once, not per recipe.
[private]
lint-deps:
    poetry install --only lint --no-root

[private]
test-deps:
    poetry install --only unit --no-root
