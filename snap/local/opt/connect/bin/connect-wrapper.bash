#!/bin/bash

set -eu

unset KAFKA_JMX_OPTS

CONF_FILE="${SNAP_COMMON}/etc/kafka/connect-distributed.properties"

if [ -z "${KAFKA_LOG4J_OPTS:-}" ]; then
    log_config="${SNAP_COMMON}/etc/kafka/connect-log4j2.yaml"
    if [ -f "${log_config}" ]; then
        export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${log_config}"
    fi
fi

exec "${SNAP}/usr/bin/setpriv" \
    --clear-groups --reuid _daemon_ --regid _daemon_ -- \
    "${SNAP}/opt/kafka/bin/connect-distributed.sh" "${CONF_FILE}"
