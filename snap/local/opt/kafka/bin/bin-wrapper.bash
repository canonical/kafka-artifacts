#!/bin/bash

set -e

unset KAFKA_JMX_OPTS
export LOG_DIR="${SNAP_COMMON}/var/log/kafka"

# Match the broker: expose side-loaded broker plugins to the tools too, so a class the daemon
# resolves does not go missing from the CLI. The JVM expands the '/*' wildcard.
export CLASSPATH="${SNAP_COMMON}/var/lib/kafka/plugins/broker/*${CLASSPATH:+:${CLASSPATH}}"

# Point tools at the rendered tools log config when present, avoiding Kafka's noisy default.
tools_log_config="${SNAP_COMMON}/etc/kafka/tools-log4j2.yaml"
if [ -z "${KAFKA_LOG4J_OPTS:-}" ] && [ -f "${tools_log_config}" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${tools_log_config}"
fi

exec "${SNAP}/opt/kafka/bin/${bin_script}" "${@}"
