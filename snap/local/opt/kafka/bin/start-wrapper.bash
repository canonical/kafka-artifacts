#!/bin/bash

set -eu

source "${SNAP}/opt/shared/bin/set-conf.sh"

CONF_FILE="${SNAP_COMMON}/etc/kafka/server.properties"
CLUSTER_ID_FILE="${SNAP_COMMON}/etc/kafka/cluster.id"
SERVER_LOG4J="${SNAP_COMMON}/etc/kafka/log4j2.yaml"

# where meta.properties is
first_log_dir() {
    local dirs
    dirs="$(get_prop "${CONF_FILE}" "log.dirs")"
    if [ "${dirs}" = "null" ] || [ -z "${dirs}" ]; then
        dirs="${SNAP_COMMON}/var/lib/kafka/data"
    fi
    echo "${dirs%%,*}"
}

# cluster UUID by precedence: persisted id -> "snap set kafka cluster-id=" -> fresh default
resolve_cluster_id() {
    local configured
    if [ -s "${CLUSTER_ID_FILE}" ]; then
        cat "${CLUSTER_ID_FILE}"
        return
    fi
    configured="$(snapctl get cluster-id || true)"
    if [ -z "${configured}" ]; then
        configured="$("${SNAP}/opt/kafka/bin/kafka-storage.sh" random-uuid \
            | tail -n1 | tr -d '[:space:]')"
    fi
    printf '%s' "${configured}" > "${CLUSTER_ID_FILE}"
    chown _daemon_:root "${CLUSTER_ID_FILE}"
    echo "${configured}"
}

format_if_needed() {
    local data_dir cluster_id
    data_dir="$(first_log_dir)"

    # if storage as already formatted
    if [ -f "${data_dir}/meta.properties" ]; then
        return
    fi

    cluster_id="$(resolve_cluster_id)"

    # KRaft needs the initial controller topology
    # voters set -> static/no flag
    # controller node -> --standalone
    # pure broker -> --no-initial-controllers
    local -a topology=()
    local voters roles
    voters="$(get_prop "${CONF_FILE}" "controller.quorum.voters")"
    roles="$(get_prop "${CONF_FILE}" "process.roles")"
    if [ "${voters}" != "null" ] && [ -n "${voters}" ]; then
        :
    elif [[ ",${roles}," == *",controller,"* ]]; then
        topology=(--standalone)
    else
        topology=(--no-initial-controllers)
    fi

    echo "Formatting KRaft storage with cluster id ${cluster_id}"
    "${SNAP}/usr/bin/setpriv" \
        --clear-groups --reuid _daemon_ --regid _daemon_ -- \
        "${SNAP}/opt/kafka/bin/kafka-storage.sh" format \
        --cluster-id "${cluster_id}" \
        --config "${CONF_FILE}" \
        "${topology[@]}" \
        --ignore-formatted
}

format_if_needed

# formatting done, start broker
if [ -z "${KAFKA_LOG4J_OPTS:-}" ] && [ -f "${SERVER_LOG4J}" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${SERVER_LOG4J}"
fi

exec "${SNAP}/usr/bin/setpriv" \
    --clear-groups --reuid _daemon_ --regid _daemon_ -- \
    "${SNAP}/opt/kafka/bin/kafka-server-start.sh" "${CONF_FILE}"
