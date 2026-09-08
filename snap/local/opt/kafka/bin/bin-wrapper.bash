#!/bin/bash

set -e

unset KAFKA_JMX_OPTS
export LOG_DIR="${SNAP_COMMON}/var/log/kafka"

# Point tools at the rendered tools log config when present, avoiding Kafka's noisy default.
tools_log_config="${SNAP_COMMON}/etc/kafka/tools-log4j2.yaml"
if [ -z "${KAFKA_LOG4J_OPTS:-}" ] && [ -f "${tools_log_config}" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${tools_log_config}"
fi

exec "${SNAP}/opt/kafka/bin/${bin_script}" "${@}"
